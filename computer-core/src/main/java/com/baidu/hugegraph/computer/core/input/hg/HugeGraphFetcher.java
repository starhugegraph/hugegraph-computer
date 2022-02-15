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

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.config.OptionSpace;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.GraphFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.computer.core.input.VertexFetcher;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class HugeGraphFetcher implements GraphFetcher {

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
    private final InputSplitRpcService rpcService;

    public HugeGraphFetcher(Config config, InputSplitRpcService rpcService) {
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
        this.rpcService = rpcService;
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
        return this.rpcService.nextVertexInputSplit();
    }

    @Override
    public InputSplit nextEdgeInputSplit() {
        return this.rpcService.nextEdgeInputSplit();
    }
}
