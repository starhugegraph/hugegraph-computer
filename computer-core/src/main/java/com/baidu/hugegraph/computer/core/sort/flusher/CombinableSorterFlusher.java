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

package com.baidu.hugegraph.computer.core.sort.flusher;

import java.io.IOException;
import java.util.Iterator;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.computer.core.dataparser.DataParser;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InlinePointer;
import com.baidu.hugegraph.computer.core.util.BytesUtil;

public abstract class CombinableSorterFlusher {

    private final Combiner<Pointer> combiner;

    public CombinableSorterFlusher(Combiner<Pointer> combiner) {
        this.combiner = combiner;
    }

    public void flushBytes(Iterator<byte[]> datas) throws IOException {
        byte[] last = datas.next();
        byte[] combineData = DataParser.parseValue(last);
        Pointer combineValue = new InlinePointer(combineData);

        while (true) {
            byte[] current = null;
            if (datas.hasNext()) {
                current = datas.next();
                if (BytesUtil.compareKey(last, current) == 0) {
                    byte[] currentData = DataParser.parseValue(current);
                    Pointer currentValue = new InlinePointer(currentData);
                    this.combiner.combine(combineValue, currentValue);
                    continue;
                }
            }
            byte[] keyData = DataParser.parseKey(last);
            Pointer lastKey = new InlinePointer(keyData);
            this.writeKvEntry(new DefaultKvEntry(lastKey, combineValue));

            if (current == null) {
                break;
            }

            last = current;
            combineData = DataParser.parseValue(last);
            combineValue = new InlinePointer(combineData);
        }
    }

    public void flush(Iterator<KvEntry> entries) throws IOException {
        E.checkArgument(entries.hasNext(),
                        "Parameter entries can't be empty");

        KvEntry last = entries.next();
        Pointer combineValue = last.value();

        while (true) {
            KvEntry current = null;
            if (entries.hasNext()) {
                current = entries.next();
                if (last.compareTo(current) == 0) {
                    combineValue = this.combiner.combine(combineValue,
                                                         current.value());
                    continue;
                }
            }

            this.writeKvEntry(new DefaultKvEntry(last.key(), combineValue));

            if (current == null) {
                break;
            }

            last = current;
            combineValue = last.value();
        }
    }

    protected abstract void writeKvEntry(KvEntry entry) throws IOException;
}
