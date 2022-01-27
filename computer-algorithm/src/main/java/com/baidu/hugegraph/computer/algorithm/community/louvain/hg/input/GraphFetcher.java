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

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.structure.graph.Vertex;
import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.HugeClientBuilder;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Shard;

public class GraphFetcher implements Closeable {

    private final HugeClient client;
    private final List<Shard> shards;
    private final HugeEdgeFetcher hugeEdgeFetcher;
    private int shardPosition;

    private final List<Shard> shardsVertex;
    private final HugeVertexFetcher hugeVertexFetcher;
    private int shardPositionVertex;

    public GraphFetcher(Config config) {
        String url = config.get(ComputerOptions.HUGEGRAPH_URL);
        String graph = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        String usrname = config.get(ComputerOptions.AUTH_USRNAME);
        String passwd = config.get(ComputerOptions.AUTH_PASSWD);

        HugeClientBuilder clientBuilder = new HugeClientBuilder(url, graph);
        if (StringUtils.isNotBlank(usrname)) {
            this.client = clientBuilder.configUser(usrname, passwd).build();
        } else {
            this.client = clientBuilder.build();
        }
        try {
            long splitsSize = config.get(ComputerOptions.INPUT_SPLITS_SIZE);
            this.shards = this.client.traverser().edgeShards(splitsSize);
            this.hugeEdgeFetcher = new HugeEdgeFetcher(config, this.client);
            this.shardPosition = 0;

            this.shardsVertex = this.client.traverser()
                    .vertexShards(splitsSize);
            this.hugeVertexFetcher = new HugeVertexFetcher(config, this.client);
            this.shardPositionVertex = 0;
        } catch (Throwable e) {
            this.client.close();
            throw e;
        }
    }

    public Shard nextEdgeShard() {
        if (shardPosition < this.shards.size()) {
            return this.shards.get(shardPosition++);
        } else {
            return null;
        }
    }

    public Iterator<Edge> createIteratorFromEdge() {
        return new IteratorFromEdge();
    }

    private class IteratorFromEdge implements Iterator<Edge> {

        private Shard currentShard;

        public IteratorFromEdge() {
            this.currentShard = null;
        }

        @Override
        public boolean hasNext() {
            while (this.currentShard == null || !hugeEdgeFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.currentShard = GraphFetcher.this.nextEdgeShard();
                if (this.currentShard == null) {
                    return false;
                }
                hugeEdgeFetcher.prepareLoadShard(this.currentShard);
            }
            return true;
        }

        @Override
        public Edge next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return hugeEdgeFetcher.next();
        }
    }


    public Shard nextVertexShard() {
        if (shardPositionVertex < this.shardsVertex.size()) {
            return this.shardsVertex.get(shardPositionVertex++);
        } else {
            return null;
        }
    }

    public Iterator<Vertex> createIteratorFromVertex() {
        return new IteratorFromVertex();
    }

    private class IteratorFromVertex implements Iterator<Vertex> {

        private Shard currentShard;

        public IteratorFromVertex() {
            this.currentShard = null;
        }

        @Override
        public boolean hasNext() {
            while (this.currentShard == null || !hugeVertexFetcher.hasNext()) {
                /*
                 * The first time or the current split is complete,
                 * need to fetch next input split meta
                 */
                this.currentShard = GraphFetcher.this.nextVertexShard();
                if (this.currentShard == null) {
                    return false;
                }
                hugeVertexFetcher.prepareLoadShard(this.currentShard);
            }
            return true;
        }

        @Override
        public Vertex next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return hugeVertexFetcher.next();
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
