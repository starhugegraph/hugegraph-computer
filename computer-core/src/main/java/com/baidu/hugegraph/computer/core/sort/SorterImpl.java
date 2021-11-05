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

package com.baidu.hugegraph.computer.core.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIteratorAdaptor;
import com.baidu.hugegraph.computer.core.sort.merge.HgkvDirMerger;
import com.baidu.hugegraph.computer.core.sort.merge.HgkvDirMergerImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.InputSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorterImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.JavaInputSorter;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesWithFirstSubKvInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InputToEntries;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDir4SubKvReaderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.select.DisperseEvenlySelector;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.select.InputFilesSelector;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.select.SelectedFiles;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

public class SorterImpl implements Sorter {

    private static final Logger LOG = Log.logger(SorterImpl.class);
    private final Config config;

    public SorterImpl(Config config) {
        this.config = config;
    }

    @Override
    public void sortBuffer(RandomAccessInput input, InnerSortFlusher flusher,
                           boolean withSubKv) throws Exception {
        try (EntryIterator entries = new KvEntriesInput(input, withSubKv)) {
            InputSorter sorter = new JavaInputSorter();
            StopWatch watch = new StopWatch();
            watch.start();
            flusher.flush(sorter.sort(entries));
            watch.stop();
            logBuffer(input, watch.getNanoTime(), withSubKv);
        }
    }

    private void logBuffer(RandomAccessInput input, long time,
                           boolean withSubKv) throws IOException {
        long numEntries = 0L;
        long subKvEntries = 0L;
        input.seek(0L);
        EntryIterator entries;
        if (withSubKv) {
            entries = new KvEntriesWithFirstSubKvInput(input);
        } else {
            entries = new KvEntriesInput(input);
        }
        while (entries.hasNext()) {
            KvEntry next = entries.next();
            numEntries++;
            subKvEntries += next.numSubEntries();
        }

        if (withSubKv) {
            LOG.info("SorterImpl sort edge buffer cost: {} ,num: {} ,sub: {}",
                      time, numEntries, subKvEntries);
        } else {
            LOG.info("SorterImpl sort vertex buffer cost: {} ,num: {} ,sub: {}",
                      time, numEntries, subKvEntries);
        }
    }

    @Override
    public void mergeBuffers(List<RandomAccessInput> inputs,
                             OuterSortFlusher flusher, String output,
                             boolean withSubKv) throws Exception {
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

        long time = this.sortBuffers(entries, flusher, output);
        this.logBuffers(inputs, withSubKv, time);
    }

    @Override
    public void mergeInputs(List<String> inputs, OuterSortFlusher flusher,
                            List<String> outputs, boolean withSubKv)
                            throws Exception {
        InputToEntries inputToEntries;
        if (withSubKv) {
            inputToEntries = o -> new HgkvDir4SubKvReaderImpl(o).iterator();
        } else {
            inputToEntries = o -> new HgkvDirReaderImpl(o).iterator();
        }
        this.mergeInputs(inputs, inputToEntries, flusher, outputs);
    }

    @Override
    public PeekableIterator<KvEntry> iterator(List<String> inputs,
                                              boolean withSubKv)
                                              throws IOException {
        InputsSorterImpl sorter = new InputsSorterImpl();
        List<EntryIterator> entries = new ArrayList<>();
        for (String input : inputs) {
            HgkvDirReader reader = new HgkvDirReaderImpl(input, false,
                                                         withSubKv);
            entries.add(reader.iterator());
        }
        return PeekableIteratorAdaptor.of(sorter.sort(entries));
    }

    private long sortBuffers(List<EntryIterator> entries,
                             OuterSortFlusher flusher, String output)
                             throws IOException {
        StopWatch watch = new StopWatch();
        InputsSorter sorter = new InputsSorterImpl();
        try (HgkvDirBuilder builder = new HgkvDirBuilderImpl(this.config,
                                                             output)) {
            watch.start();
            EntryIterator result = sorter.sort(entries);
            flusher.flush(result, builder);
            watch.stop();
        }
        return watch.getTime();
    }

    private void logBuffers(List<RandomAccessInput> inputs,
                            boolean withSubKv, long time)
                            throws IOException {
        for (RandomAccessInput input : inputs) {
                input.seek(0L);
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
            LOG.info("SortImpl merge edge buffers cost: {} ,numEntries: {} " +
                     ",edgeTotal: {} ", time, numEntries, subKvEntries);
        } else {
            LOG.info("SortImpl merge vertex buffers cost: {} ,numEntries: {} " +
                     ",edgeTotal: {} ", time, numEntries, subKvEntries);
        }
    }

    private void mergeInputs(List<String> inputs, InputToEntries inputToEntries,
                             OuterSortFlusher flusher, List<String> outputs)
                             throws Exception {
        InputFilesSelector selector = new DisperseEvenlySelector();
        // Each SelectedFiles include some input files per output.
        List<SelectedFiles> results = selector.selectedOfOutputs(inputs,
                                                                 outputs);

        HgkvDirMerger merger = new HgkvDirMergerImpl(this.config);
        for (SelectedFiles result : results) {
            merger.merge(result.inputs(), inputToEntries,
                         result.output(), flusher);
        }
    }
}
