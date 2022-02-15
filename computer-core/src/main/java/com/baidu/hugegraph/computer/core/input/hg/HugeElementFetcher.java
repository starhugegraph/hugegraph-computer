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

package com.baidu.hugegraph.computer.core.input.hg;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.ElementFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;

public abstract class HugeElementFetcher<T> implements ElementFetcher<T> {

    private final Config config;
    private final HugeGraph graph;
    private Iterator<T> localBatch;
    private T next;

    public HugeElementFetcher(Config config, HugeGraph graph) {
        this.config = config;
        this.graph = graph;
    }

    protected Config config() {
        return this.config;
    }

    protected HugeGraph graph() {
        return this.graph;
    }

    @Override
    public void prepareLoadInputSplit(InputSplit split) {
        this.localBatch = this.fetch(split);
    }

    @Override
    public boolean hasNext() {
        if (this.next != null) {
            return true;
        }
        if (this.localBatch != null && this.localBatch.hasNext()) {
            this.next = this.localBatch.next();
            return true;
        } else {
            this.localBatch = null;
            return false;
        }
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        T current = this.next;
        this.next = null;
        return current;
    }

    public abstract Iterator<T> fetch(InputSplit split);

    public static Shard toShard(InputSplit split) {
        return new Shard(split.start(), split.end(), 0);
    }
}
