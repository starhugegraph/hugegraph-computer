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

package com.baidu.hugegraph.computer.core.sort.sorter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.sort.sorting.LoserTreeInputsSorting;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.util.E;

public class SubKvSorter implements EntryIterator {

    private final PeekableIterator<KvEntry> entries;
    private final int subKvSortPathNum;
    private final List<EntryIterator> subKvMergeSources;
    private Iterator<KvEntry> subKvSorting;
    private KvEntry currentEntry;

    public SubKvSorter(PeekableIterator<KvEntry> entries,
                       int subKvSortPathNum) throws Exception {
        E.checkArgument(entries.hasNext(),
                        "Parameter entries can't be empty");
        E.checkArgument(subKvSortPathNum > 0,
                        "Parameter subKvSortPathNum must be > 0");
        this.entries = entries;
        this.subKvSortPathNum = subKvSortPathNum;
        this.subKvMergeSources = new ArrayList<>(this.subKvSortPathNum);
        this.init();
    }

    @Override
    public boolean hasNext() {
        return this.subKvSorting.hasNext();
    }

    @Override
    public KvEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        return this.subKvSorting.next();
    }


    @Override
    public void close() throws Exception {
        for (EntryIterator mergePath : this.subKvMergeSources) {
            mergePath.close();
        }
    }

    public KvEntry currentKv() {
        return this.currentEntry;
    }

    public void reset() throws Exception {
        if (!this.entries.hasNext()) {
            this.currentEntry = null;
            return;
        }
        this.subKvMergeSources.clear();

        assert this.subKvSortPathNum > 0;
        KvEntry entry;
        while (true) {
            entry = this.entries.next();
            this.subKvMergeSources.add(new MergePath(this.entries, entry));

            KvEntry next = this.entries.peek();
            if (this.subKvMergeSources.size() == this.subKvSortPathNum ||
                next == null || entry.key().compareTo(next.key()) != 0) {
                break;
            }
        }
        this.subKvSorting = new LoserTreeInputsSorting<>(
                            this.subKvMergeSources);
        this.currentEntry = entry;
    }

    private void init() throws Exception {
        this.reset();
    }

    private static class MergePath implements EntryIterator {

        private final PeekableIterator<KvEntry> entries;
        private EntryIterator subKvIter;
        private KvEntry currentEntry;

        public MergePath(PeekableIterator<KvEntry> entries,
                         KvEntry currentEntry) {
            this.entries = entries;
            this.subKvIter = EntriesUtil.subKvIterFromEntry(currentEntry);
            this.currentEntry = currentEntry;
        }

        @Override
        public boolean hasNext() {
            if (this.subKvIter.hasNext()) {
                return true;
            }

            return this.fetchNextPath();
        }

        @Override
        public KvEntry next() {
            return this.subKvIter.next();
        }

        @Override
        public void close() throws Exception {
            this.subKvIter.close();
        }

        private boolean fetchNextPath() {
            KvEntry nextEntry = this.entries.peek();
            if (nextEntry != null &&
                nextEntry.key().compareTo(this.currentEntry.key()) == 0) {
                this.currentEntry = this.entries.next();
                this.subKvIter = EntriesUtil.subKvIterFromEntry(
                                             this.currentEntry);
            }
            return this.subKvIter.hasNext();
        }
    }
}
