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

package com.baidu.hugegraph.computer.algorithm.community.louvain.hg.input;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.structure.graph.Shard;

public abstract class HugeElementFetcher<T> implements Iterator<T> {

    private final Config config;
    private final HugeClient client;
    private Iterator<T> localBatch;
    private T next;

    public HugeElementFetcher(Config config, HugeClient client) {
        this.config = config;
        this.client = client;
    }

    protected HugeClient client() {
        return this.client;
    }

    protected int pageSize() {
        return this.config.get(ComputerOptions.INPUT_SPLIT_PAGE_SIZE);
    }

    public void prepareLoadShard(Shard shard) {
        this.localBatch = this.fetch(shard);
    }

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

    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        T current = this.next;
        this.next = null;
        return current;
    }

    protected abstract Iterator<T> fetch(Shard shard);
}
