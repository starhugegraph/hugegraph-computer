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

package com.baidu.hugegraph.computer.algorithm.community.louvain.input;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
//import com.baidu.hugegraph.computer.core.input.InputSplitFetcher;
import com.baidu.hugegraph.computer.core.input.VertexFetcher;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.MasterInputHandler;
import com.baidu.hugegraph.computer.core.input.GraphFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;
//import com.baidu.hugegraph.computer.core.input.hg.HugeInputSplitFetcher;
import com.baidu.hugegraph.config.OptionSpace;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
//import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.hg.HugeVertexFetcher;
import com.baidu.hugegraph.computer.core.input.hg.HugeEdgeFetcher;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class HugeGraphFetcherLocal implements GraphFetcher {

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
    private final VertexFetcher vertexFetcher;
    private final EdgeFetcher edgeFetcher;
    private MasterInputHandler handler;

    public HugeGraphFetcherLocal(Config config, MasterInputHandler handler) {
        Map<String, Object> configs = new HashMap<>();
        String pdPeers = config.get(ComputerOptions.INPUT_PD_PEERS);

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

        propConfig.setProperty(CoreOptions.GRAPH_SPACE.name(),
                parts[0]);
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

        this.vertexFetcher = new HugeVertexFetcher(config, this.hugeGraph);
        this.edgeFetcher = new HugeEdgeFetcher(config, this.hugeGraph);

        this.handler = handler;
    }

    @Override
    public void close() {
        // pass
        try {
            this.hugeGraph.close();
        } catch (Throwable e) {
            LOG.error("Exception occur when close graph", e);
        }
    }

    @Override
    public VertexFetcher vertexFetcher() {
        return this.vertexFetcher;
    }

    @Override
    public EdgeFetcher edgeFetcher() {
        return this.edgeFetcher;
    }

    @Override
    public InputSplit nextVertexInputSplit() {
        return this.handler.nextVertexInputSplit();
    }

    @Override
    public InputSplit nextEdgeInputSplit() {
        return this.handler.nextEdgeInputSplit();
    }


    public Iterator<HugeEdge> createIteratorFromEdge() {
        return new IteratorFromEdge();
    }

    private class IteratorFromEdge implements Iterator<HugeEdge> {

        private InputSplit currentSplit;

        public IteratorFromEdge() {
            this.currentSplit = null;
        }

        @Override
        public boolean hasNext() {
            EdgeFetcher edgeFetcher = HugeGraphFetcherLocal.this.edgeFetcher;
            try {
                while (this.currentSplit == null || !edgeFetcher.hasNext()) {
                    /*
                     * The first time or the current split is complete,
                     * need to fetch next input split meta
                     */
                    this.currentSplit =
                            HugeGraphFetcherLocal.this.nextEdgeInputSplit();
                    if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                        return false;
                    }
                    edgeFetcher.prepareLoadInputSplit(this.currentSplit);
                }
            } catch (Exception e) {
            }
            return true;
        }

        @Override
        public HugeEdge next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return (HugeEdge)HugeGraphFetcherLocal.this.edgeFetcher.next();
        }
    }

    public Iterator<HugeVertex> createIteratorFromVertex() {
        return new IteratorFromVertex();
    }

    private class IteratorFromVertex implements Iterator<HugeVertex> {

        private InputSplit currentSplit;

        public IteratorFromVertex() {
            this.currentSplit = null;
        }

        @Override
        public boolean hasNext() {
            VertexFetcher vertexFetcher =
                    HugeGraphFetcherLocal.this.vertexFetcher;
            try {
                while (this.currentSplit == null || !vertexFetcher.hasNext()) {
                    /*
                     * The first time or the current split is complete,
                     * need to fetch next input split meta
                     */
                    this.currentSplit =
                            HugeGraphFetcherLocal.this.nextVertexInputSplit();
                    if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                        return false;
                    }
                    vertexFetcher.prepareLoadInputSplit(this.currentSplit);
                }
            } catch (Exception e) {
            }
            return true;
        }

        @Override
        public HugeVertex next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return (HugeVertex)HugeGraphFetcherLocal.this.vertexFetcher.next();
        }
    }
}
