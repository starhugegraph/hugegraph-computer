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
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
//import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
//import com.baidu.hugegraph.structure.graph.Vertex;
//import com.baidu.hugegraph.structure.graph.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

public class GraphFetcher implements Closeable {
    private static final Logger LOG = Log.logger("huge fetcher");

    static {
        // Register config
        OptionSpace.register("hstore",
                "com.baidu.hugegraph.backend.store.hstore.HstoreOptions");
        // Register backend
        BackendProviderFactory.register("hstore",
                "com.baidu.hugegraph.backend.store.hstore.HstoreProvider");

        Query.defaultCapacity(Integer.MAX_VALUE);
    }

    private final HugeGraph hugeGraph;
    //private final List<Shard> shards;
    private final HugeEdgeFetcher hugeEdgeFetcher;
    private int shardPosition;

   //private final List<Shard> shardsVertex;
    private final HugeVertexFetcher hugeVertexFetcher;
    private int shardPositionVertex;

    private PDClient pdClient;
    private List<Metapb.Partition> partitions;
    private static final String GRAPH_SUFFIX = "/g";

    public GraphFetcher(Config config) {
        String pdPeers = config.get(ComputerOptions.INPUT_PD_PEERS);

        Map<String, Object> configs = new HashMap<>();
        // TODO: add auth check
        configs.put("pd.peers", pdPeers);
        configs.put("backend", "hstore");
        configs.put("serializer", "binary");
        configs.put("search.text_analyzer", "jieba");
        configs.put("search.text_analyzer_mode", "INDEX");
        configs.put("gremlin.graph", "com.baidu.hugegraph.HugeFactory");

        Configuration propConfig = new MapConfiguration(configs);
        String graph = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        String[] parts = graph.split("/");

        propConfig.setProperty(CoreOptions.STORE.name(),
                parts[parts.length - 1]);
        HugeConfig hugeConfig = new HugeConfig(propConfig);
        try {
            this.hugeGraph = (HugeGraph) GraphFactory.open(hugeConfig);
        } catch (Throwable e) {
            LOG.error("Exception occur when open graph", e);
            throw e;
        }
        this.hugeGraph.graphSpace(parts[0]);
        this.hugeVertexFetcher = new HugeVertexFetcher(config, this.hugeGraph);
        this.hugeEdgeFetcher = new HugeEdgeFetcher(config, this.hugeGraph);

        this.pdClient = PDClient.create(
                PDConfig.of(pdPeers).setEnablePDNotify(true));
        this.partitions = new ArrayList<>();
        this.shardPosition = 0;
        this.shardPositionVertex = 0;
        try {
            if (this.partitions == null || this.partitions.isEmpty()) {
                this.partitions = this.pdClient.getPartitions(
                        0, graph + GRAPH_SUFFIX);
            }
        } catch (PDException e) {
            e.printStackTrace();
        }

    }

    public Metapb.Partition nextEdgeShard() {
        if (shardPosition < this.partitions.size()) {
            return this.partitions.get(shardPosition++);
        } else {
            return null;
        }
    }

    public Iterator<Edge> createIteratorFromEdge() {
        return new IteratorFromEdge();
    }

    private class IteratorFromEdge implements Iterator<Edge> {

        private Metapb.Partition currentShard;

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


    public Metapb.Partition nextVertexShard() {
        if (shardPositionVertex < this.partitions.size()) {
            return this.partitions.get(shardPositionVertex++);
        } else {
            return null;
        }
    }

    public Iterator<Vertex> createIteratorFromVertex() {
        return new IteratorFromVertex();
    }

    private class IteratorFromVertex implements Iterator<Vertex> {

        private Metapb.Partition currentShard;

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
        if (hugeGraph != null) {
            try {
                this.hugeGraph.close();
            } catch (Throwable e) {
                LOG.error("Exception occur when close graph", e);
            }
        }
    }
}
