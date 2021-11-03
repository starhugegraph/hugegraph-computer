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

package com.baidu.hugegraph.computer.core.sort.sorting;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesWithFirstSubKvInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.combiner.OverwriteCombiner;
import com.baidu.hugegraph.computer.core.combiner.PointerCombiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.sender.WriteBuffers;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineSubKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public abstract class SortManager implements Manager {

    public static final Logger LOG = Log.logger(SortManager.class);

    private final ComputerContext context;
    private final ExecutorService sortExecutor;
    private final Sorter sorter;
    private final int capacity;
    private final int flushThreshold;

    public SortManager(ComputerContext context) {
        this.context = context;
        Config config = context.config();
        this.sortExecutor = ExecutorUtil.newFixedThreadPool(
                            this.threadNum(config), this.threadPrefix());
        this.sorter = new SorterImpl(config);
        this.capacity = config.get(
                        ComputerOptions.WORKER_WRITE_BUFFER_INIT_CAPACITY);
        this.flushThreshold = config.get(
                              ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
    }

    @Override
    public abstract String name();

    protected abstract String threadPrefix();

    protected Integer threadNum(Config config) {
        return config.get(ComputerOptions.SORT_THREAD_NUMS);
    }

    @Override
    public void init(Config config) {
        // pass
    }

    @Override
    public void close(Config config) {
        this.sortExecutor.shutdown();
        try {
            this.sortExecutor.awaitTermination(Constants.SHUTDOWN_TIMEOUT,
                                               TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted when waiting sort executor terminated");
        }
    }

    public CompletableFuture<ByteBuffer> sort(MessageType type,
                                              WriteBuffers buffer) {
        return CompletableFuture.supplyAsync(() -> {
            RandomAccessInput bufferForRead = buffer.wrapForRead();
            // TODO：This ByteBuffer should be allocated from the off-heap
            BytesOutput output = IOFactory.createBytesOutput(this.capacity);
            InnerSortFlusher flusher = this.createSortFlusher(
                                       type, output,
                                       this.flushThreshold);
            try {
                StopWatch watch = new StopWatch();
                watch.start();
                this.sorter.sortBuffer(bufferForRead, flusher,
                                       type == MessageType.EDGE);
                watch.stop();
                this.logBuffer(bufferForRead, type, watch.getNanoTime());
            } catch (Exception e) {
                throw new ComputerException("Failed to sort buffers of %s " +
                                            "message", e, type.name());
            }

            return ByteBuffer.wrap(output.buffer(), 0, (int) output.position());
        }, this.sortExecutor);
    }

    private void logBuffer(RandomAccessInput input, MessageType type,
                           long time) {
        long numEntries = 0L;
        long subKvEntries = 0L;
        try {
            input.seek(0L);
        } catch (IOException e) {
            e.printStackTrace();
        }
        EntryIterator entries;
        if (type == MessageType.EDGE) {
            entries = new KvEntriesWithFirstSubKvInput(input);
        } else {
            entries = new KvEntriesInput(input);
        }
        while (entries.hasNext()) {
            KvEntry next = entries.next();
            numEntries++;
            subKvEntries += next.numSubEntries();
        }

        if (type == MessageType.EDGE) {
            LOG.info("Sort edge buffer cost: {} ns, numEntries: {} ," +
                            " edgeTotal: {} ",
                    time, numEntries, subKvEntries);
        } else {
            LOG.info("Sort vertex buffer cost: {} ns, numEntries: {} ," +
                            " edgeTotal: {} ",
                    time, numEntries, subKvEntries);
        }
    }

    public CompletableFuture<Void> mergeBuffers(List<RandomAccessInput> inputs,
                                                String path,
                                                boolean withSubKv,
                                                OuterSortFlusher flusher) {
        return CompletableFuture.runAsync(() -> {
            if (withSubKv) {
                flusher.sources(inputs.size());
            }
            try {
                StopWatch watch = new StopWatch();
                watch.start();
                this.sorter.mergeBuffers(inputs, flusher, path, withSubKv);
                watch.stop();
                this.logBuffers(inputs, withSubKv, watch.getTime());
            } catch (Exception e) {
                throw new ComputerException(
                          "Failed to merge %s buffers to file '%s'",
                          e, inputs.size(), path);
            }
        }, this.sortExecutor);
    }

    private void logBuffers(List<RandomAccessInput> inputs,
                            boolean withSubKv, long time) {
        for (RandomAccessInput input : inputs) {
            try {
                input.seek(0L);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        long numEntries = 0L;
        long subKvEntries = 0L;
        List<EntryIterator> entries;
        if (withSubKv) {
            entries = inputs.stream()
                    .map(KvEntriesWithFirstSubKvInput::new)
                    .collect(Collectors.toList());
        } else {
            entries = inputs.stream()
                    .map(KvEntriesInput::new)
                    .collect(Collectors.toList());
        }
        for (EntryIterator iter : entries) {
            while (iter.hasNext()) {
                KvEntry next = iter.next();
                numEntries++;
                subKvEntries += next.numSubEntries();
            }
        }

        if (withSubKv) {
            LOG.info("Merge edge buffers cost: {} ms, numEntries: {} ," +
                            " edgeTotal: {} ",
                     time, numEntries, subKvEntries);
        } else {
            LOG.info("Merge vertex buffers cost: {} ms, numEntries: {} ," +
                            " edgeTotal: {} ",
                    time, numEntries, subKvEntries);
        }
    }

    public void mergeInputs(List<String> inputs, List<String> outputs,
                            boolean withSubKv, OuterSortFlusher flusher) {
        if (withSubKv) {
            flusher.sources(inputs.size());
        }
        try {
            int inputsSize = inputs.size();
            StopWatch watch = new StopWatch();
            watch.start();
            this.sorter.mergeInputs(inputs, flusher, outputs, withSubKv);
            watch.stop();
            this.logMergeInputs(inputs, watch.getTime(), inputsSize);
        } catch (Exception e) {
            throw new ComputerException(
                      "Failed to merge %s files into %s files",
                      e, inputs.size(), outputs.size());
        }
    }

    public void logMergeInputs(List<String> inputs, long time, int inputSize) {
        long numEntries = 0L;
        long subKvEntries = 0L;
        for (String input : inputs) {
            try {
                HgkvDir dir = HgkvDirImpl.open(input);
                numEntries += dir.numEntries();
                subKvEntries += dir.numSubEntries();
                dir.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        LOG.info("Merge inputs cost: {} ms, numEntries: {} , edgeTotal: {} ," +
                        "inputsSize: {}",
                 time, numEntries, subKvEntries, inputSize);
    }

    public PeekableIterator<KvEntry> iterator(List<String> outputs,
                                              boolean withSubKv) {
        try {
            return this.sorter.iterator(outputs, withSubKv);
        } catch (IOException e) {
            throw new ComputerException("Failed to iterate files: '%s'",
                                        outputs);
        }
    }

    private InnerSortFlusher createSortFlusher(MessageType type,
                                               RandomAccessOutput output,
                                               int flushThreshold) {
        Combiner<Pointer> combiner;
        boolean needSortSubKv;

        switch (type) {
            case VERTEX:
                combiner = this.createVertexCombiner();
                needSortSubKv = false;
                break;
            case EDGE:
                combiner = this.createEdgeCombiner();
                needSortSubKv = true;
                break;
            case MSG:
                combiner = this.createMessageCombiner();
                needSortSubKv = false;
                break;
            default:
                throw new ComputerException("Unsupported combine message " +
                                            "type for %s", type);
        }

        InnerSortFlusher flusher;
        if (combiner == null) {
            flusher = new KvInnerSortFlusher(output);
        } else {
            if (needSortSubKv) {
                flusher = new CombineSubKvInnerSortFlusher(output, combiner,
                                                           flushThreshold);
            } else {
                flusher = new CombineKvInnerSortFlusher(output, combiner);
            }
        }

        return flusher;
    }

    private Combiner<Pointer> createVertexCombiner() {
        Config config = this.context.config();
        Combiner<Properties> propCombiner = config.createObject(
                ComputerOptions.WORKER_VERTEX_PROPERTIES_COMBINER_CLASS);

        return this.createPropertiesCombiner(propCombiner);
    }

    private Combiner<Pointer> createEdgeCombiner() {
        Config config = this.context.config();
        Combiner<Properties> propCombiner = config.createObject(
                ComputerOptions.WORKER_EDGE_PROPERTIES_COMBINER_CLASS);

        return this.createPropertiesCombiner(propCombiner);
    }

    private Combiner<Pointer> createMessageCombiner() {
        Config config = this.context.config();
        Combiner<Value<?>> valueCombiner = config.createObject(
                           ComputerOptions.WORKER_COMBINER_CLASS, false);

        if (valueCombiner == null) {
            return null;
        }

        Value<?> v1 = config.createObject(
                      ComputerOptions.ALGORITHM_MESSAGE_CLASS);
        Value<?> v2 = v1.copy();
        return new PointerCombiner<>(v1, v2, valueCombiner);
    }

    private Combiner<Pointer> createPropertiesCombiner(
                              Combiner<Properties> propCombiner) {
        /*
         * If propertiesCombiner is OverwriteCombiner, just remain the
         * second, no need to deserialize the properties and then serialize
         * the second properties.
         */
        Combiner<Pointer> combiner;
        if (propCombiner instanceof OverwriteCombiner) {
            combiner = new OverwriteCombiner<>();
        } else {
            GraphFactory graphFactory = this.context.graphFactory();
            Properties v1 = graphFactory.createProperties();
            Properties v2 = graphFactory.createProperties();

            combiner = new PointerCombiner<>(v1, v2, propCombiner);
        }
        return combiner;
    }
}
