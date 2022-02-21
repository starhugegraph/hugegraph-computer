/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.computer.core.compute;

import java.io.File;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.store.FileGenerator;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvManager;
import com.baidu.hugegraph.computer.core.receiver.MessageStat;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.util.Consumers;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.TimeUtil;

import java.util.ArrayList;
import java.util.List;

public class ComputeManager {

    private static final Logger LOG = Log.logger(ComputeManager.class);
    private static final String PREFIX = "partition-compute-executor-%s";

    private final ComputerContext context;
    private final Managers managers;

    private final Map<Integer, FileGraphPartition> partitions;
    private final MessageRecvManager recvManager;
    private final MessageSendManager sendManager;
    private final ExecutorService partitionExecutor;

    public ComputeManager(ComputerContext context, Managers managers) {
        this.context = context;
        this.managers = managers;
        this.partitions = new HashMap<>();
        this.recvManager = this.managers.get(MessageRecvManager.NAME);
        this.sendManager = this.managers.get(MessageSendManager.NAME);
        int threadNum = this.threadNum(context.config());
        this.partitionExecutor = ExecutorUtil.newFixedThreadPool(threadNum,
                                                                 PREFIX);
        LOG.info("Created parallel partition executor for thread number: {}",
                 threadNum);
    }

    private Integer threadNum(Config config) {
        return config.get(ComputerOptions.PARTITIONS_COMPUTE_THREAD_NUMS);
    }

    public WorkerStat input(String mode) {
        WorkerStat workerStat = new WorkerStat();

        File infoFile;
        FileGenerator fileGenerator = this.managers.get(FileManager.NAME);
        String fileName = fileGenerator.fixDirectory(0, "info");
        infoFile = new File(fileName);

        if (mode == "compute") {
            try {
                List<String> partitions = FileUtils.readLines(infoFile, 
                                                              "UTF-8");
                for (String partline : partitions) {
                    String[] partstr = partline.split(" ");
                    int partition = Integer.valueOf(partstr[0]);
                    long vCount = Long.valueOf(partstr[1]);
                    long eCount = Long.valueOf(partstr[2]);
                    FileGraphPartition part;
                    part = new FileGraphPartition(this.context,
                                        this.managers, partition, mode);
                    part.setVertexCount(vCount);
                    part.setEdgeCount(eCount);
                    PartitionStat partitionStat = 
                            new PartitionStat(partition, 
                                              vCount, eCount, 0L);
                    workerStat.add(partitionStat);
                    this.partitions.put(partition, part);
                }
            }
            catch (IOException e) {
                throw new ComputerException("Error in write partition info", e);
            }
            return workerStat;
        }
  
        this.recvManager.waitReceivedAllMessages();

        Map<Integer, PeekableIterator<KvEntry>> vertices =
                     this.recvManager.vertexPartitions(this.partitionExecutor);
        Map<Integer, PeekableIterator<KvEntry>> edges =
        this.recvManager.edgePartitions(this.partitionExecutor);

        // TODO: parallel input process
        List<String> partRecords = new ArrayList<String>();
        for (Map.Entry<Integer, PeekableIterator<KvEntry>> entry :
             vertices.entrySet()) {
            int partition = entry.getKey();
            PeekableIterator<KvEntry> vertexIter = entry.getValue();
            PeekableIterator<KvEntry> edgesIter =
                                      edges.getOrDefault(
                                            partition,
                                            PeekableIterator.emptyIterator());

            FileGraphPartition part = new FileGraphPartition(this.context,
                                                this.managers, partition, mode);

            PartitionStat partitionStat = null;
            ComputerException inputException = null;
            try {
                partitionStat = part.input(vertexIter, edgesIter);
            } catch (ComputerException e) {
                inputException = e;
            } finally {
                try {
                    vertexIter.close();
                    edgesIter.close();
                } catch (Exception e) {
                    String message = "Failed to close vertex or edge file " +
                                     "iterator";
                    ComputerException closeException = new ComputerException(
                                                           message, e);
                    if (inputException != null) {
                        inputException.addSuppressed(closeException);
                    } else {
                        throw closeException;
                    }
                }
                if (inputException != null) {
                    throw inputException;
                }
            }

            workerStat.add(partitionStat);
            this.partitions.put(partition, part);
            long vertexCount = part.getVertexCount();
            long edgeCount = part.getEdgeCount();
            partRecords.add(partition + " " + vertexCount + " " + edgeCount);
        }
        try {
            FileUtils.writeLines(infoFile, partRecords, false);
        } catch (IOException e) {
            throw new ComputerException("Error in write partition info", e);
        }
        return workerStat;
    }

    public void useVariableLengthOnly() {
        for (FileGraphPartition partition : this.partitions.values()) {
            partition.useVariableLengthOnly();
        }    
    }

    public void sendHashIdMsg(ComputationContext context) {
        this.sendManager.startSend(MessageType.HASHID);
        for (FileGraphPartition partition : this.partitions.values()) {
            partition.sendIdHash(context);
        }
        this.sendManager.finishSend(MessageType.HASHID);
    }

    public void recvHashIdMsg() {
        for (FileGraphPartition partition : this.partitions.values()) {
            partition.partitionHashId();
        }
    }

    /**
     * Get compute-messages from MessageRecvManager, then put message to
     * corresponding partition. Be called before
     * {@link MessageRecvManager#beforeSuperstep} is called.
     */
    public void takeRecvedMessages(boolean inCompute) {
        Map<Integer, PeekableIterator<KvEntry>> messages;
        if (!inCompute) {
            messages = this.recvManager.hashIdMessagePartitions(
                                        this.partitionExecutor);
        }
        else {
            messages = this.recvManager.messagePartitions(
                                        this.partitionExecutor);
        }
        for (FileGraphPartition partition : this.partitions.values()) {
            partition.messages(messages.get(partition.partition()), inCompute);
        }
    }

    public WorkerStat compute(WorkerContext context, int superstep) {
        StopWatch watcher = new StopWatch();
        watcher.start();
        LOG.info("partition parallel compute started");
        this.sendManager.startSend(MessageType.MSG);
        /*
         * Remark: The main thread can perceive the partition compute exception
         * only after all partition compute completed, and only record the last
         * exception.
         */
        Map<Integer, PartitionStat> stats = new ConcurrentHashMap<>();
        Consumer<FileGraphPartition> consumer;
        if (superstep == 0) {
            consumer = partition -> {
                PartitionStat stat = partition.compute0(context);
                stats.put(stat.partitionId(), stat);
            };
        } else {
            consumer = partition -> {
                PartitionStat stat = partition.compute(context, superstep);
                stats.put(stat.partitionId(), stat);
            };
        }
        Consumers<FileGraphPartition> consumers =
                new Consumers<>(this.partitionExecutor, consumer);
        consumers.start("partitions-compute");

        try {
            for (FileGraphPartition partition : this.partitions.values()) {
                consumers.provide(partition);
            }
            consumers.await();
        } catch (Throwable t) {
            throw new ComputerException("An exception occurred when " +
                                        "partition parallel compute", t);
        }
        this.sendManager.finishSend(MessageType.MSG);

        watcher.stop();
        LOG.info("partition parallel compute finished cost time:{}",
                 TimeUtil.readableTime(watcher.getTime()));

        // After compute and send finish signal.
        WorkerStat workerStat = new WorkerStat();
        Map<Integer, MessageStat> recvStats = this.recvManager.messageStats();
        for (Map.Entry<Integer, PartitionStat> entry :
                                               stats.entrySet()) {
            PartitionStat partStat = entry.getValue();
            int partitionId = partStat.partitionId();

            MessageStat sendStat = this.sendManager.messageStat(partitionId);
            partStat.mergeSendMessageStat(sendStat);

            MessageStat recvStat = recvStats.get(partitionId);
            if (recvStat != null) {
                partStat.mergeRecvMessageStat(recvStat);
            }

            workerStat.add(partStat);
        }
        return workerStat;
    }

    public void output() {
        Consumer<FileGraphPartition> consumer = partition -> {
            PartitionStat stat = partition.output();
            LOG.info("Output partition {} complete, stat='{}'",
                     partition.partition(), stat);
        };
        Consumers<FileGraphPartition> consumers =
                  new Consumers<>(this.partitionExecutor, consumer);

        try {
            for (FileGraphPartition partition : this.partitions.values()) {
                consumers.provide(partition);
            }
            consumers.start("partitions-output");

            consumers.await();
        } catch (Throwable t) {
            throw new ComputerException("An exception occurred when " +
                                        "partition parallel compute", t);
        }
    }

    public void close() {
        this.partitionExecutor.shutdown();
    }
}
