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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.aggregator.MasterAggrManager;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.master.MasterComputation;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvManager;
import com.baidu.hugegraph.computer.core.rpc.AggregateRpcService;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.Log;

/**
 * Used for master to control input & output (same like ComputeManager),
 *
 * TODO: Better to unify later (rename ComputerManager to WorkerComputerManager
 * and extends new abstract class ComputerManager)
 */
public class MasterComputeManager<M extends Value<M>> {

    private static final Logger LOG = Log.logger(MasterComputeManager.class);
    private static boolean OUTPUT_AGGR = true;

    // Shall we use MasterComputerContext? (need refactor)
    private final ComputerContext context;
    private final Managers managers;

    // Shall we need to support get data from partitions? (or only by workers)
    private final Map<Integer, FileGraphPartition<M>> partitions;
    private final MasterComputation computation;
    private final MessageRecvManager recvManager;
    private MasterAggrManager aggrManager;

    public MasterComputeManager(ComputerContext context, Managers managers,
                                MasterComputation computation) {
        this.context = context;
        this.managers = managers;
        this.computation = computation;
        this.partitions = new HashMap<>();
        this.recvManager = this.managers.get(MessageRecvManager.NAME);
        this.aggrManager = this.managers.get(MasterAggrManager.NAME);
    }

    // Support normal worker info (if need)
    public WorkerStat input() {
        WorkerStat workerStat = new WorkerStat();
        this.recvManager.waitReceivedAllMessages();

        Map<Integer, PeekableIterator<KvEntry>> vertices =
                     this.recvManager.vertexPartitions();
        Map<Integer, PeekableIterator<KvEntry>> edges =
                     this.recvManager.edgePartitions();
        // TODO: parallel input process
        for (Map.Entry<Integer, PeekableIterator<KvEntry>> entry :
             vertices.entrySet()) {
            int partition = entry.getKey();
            FileGraphPartition<M> part = new FileGraphPartition<>(this.context,
                                                                  this.managers,
                                                                  partition);
            PartitionStat partitionStat = part.input(entry.getValue(),
                                                     edges.get(partition));
            workerStat.add(partitionStat);
            this.partitions.put(partition, part);
        }
        return workerStat;
    }

    /**
     * Get compute-messages from MessageRecvManager, then put message to
     * corresponding partition. Be called before
     * {@link MessageRecvManager#beforeSuperstep} is called.
     */
    public void takeRecvedMessages() {
        Map<Integer, PeekableIterator<KvEntry>> messages =
                     this.recvManager.messagePartitions();
        for (FileGraphPartition<M> partition : this.partitions.values()) {
            partition.messages(messages.get(partition.partition()));
        }
    }

    /* Used only for master output, consider support:
     * - aggregate value (for each worker)
     * - partition info (Todo)
     * - worker context / info (Todo)
     * - master context / info (Todo)
     * */
    public void output() {
        LOG.info("##### Master output start here #####");
        // We need get aggregate value from worker first, then combine them
        AggregateRpcService handler = this.aggrManager.handler();
        // Output if need
        if (OUTPUT_AGGR) {
            handler.listAggregators().entrySet().forEach(aggr -> {
                LOG.info("Current aggregator name is {}, value is {}",
                         aggr.getKey(), aggr.getValue());
            });
        }

        // If we need get info from each partition, we need get msg below
        for (FileGraphPartition<M> partition : this.partitions.values()) {
            PartitionStat stat = partition.output();
            LOG.info("Output partition {} complete, stat='{}'",
                     partition.partition(), stat);
        }
        LOG.info("##### Master output end here #####");
    }
}
