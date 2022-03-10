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

package com.baidu.hugegraph.computer.core.output.hg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.testutil.Whitebox;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.AbstractComputerOutput;
import com.baidu.hugegraph.computer.core.output.hg.task.TaskManager;
import com.baidu.hugegraph.util.Log;

public abstract class HugeOutput extends AbstractComputerOutput {

    private static final Logger LOG = Log.logger(HugeOutput.class);

    private TaskManager taskManager;
    private List<org.apache.tinkerpop.gremlin.structure.Vertex> vertexBatch;
    private int batchSize;

    @Override
    public void init(Config config, int partition) {
        super.init(config, partition);

        LOG.info("Start write back partition {}", this.partition());

        this.taskManager = new TaskManager(config);
        this.vertexBatch = new ArrayList<>();
        this.batchSize = config.get(ComputerOptions.OUTPUT_BATCH_SIZE);

        SchemaTransaction stx = Whitebox.invoke(this.graph().getClass(),
                "schemaTransaction", this.graph());
        stx.initAndRegisterOlapTables();
    }

    @Override
    public void masterInit(Config config) {
        super.init(config, 1);

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
        String graphName = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        String[] parts = graphName.split("/");

        propConfig.setProperty(CoreOptions.GRAPH_SPACE.name(),
                parts[0]);
        propConfig.setProperty(CoreOptions.STORE.name(),
                parts[parts.length - 1]);
        HugeConfig hugeConfig = new HugeConfig(propConfig);
        HugeGraph graph;
        try {
            graph = (HugeGraph) GraphFactory.open(hugeConfig);
        } catch (Throwable e) {
            LOG.error("Exception occur when open graph", e);
            throw e;
        }
        graph.graphSpace(parts[0]);

        try {
            this.prepareSchema(graph);
            graph.close();
        } catch (Throwable e) {
            LOG.error("prepareSchema {}", e);
        }

    }

    public HugeGraph graph() {
        return this.taskManager.graph();
    }

    public abstract void prepareSchema(HugeGraph graph);

    @Override
    public void write(Vertex vertex) {
        this.vertexBatch.add(this.constructHugeVertex(vertex));
        if (this.vertexBatch.size() >= this.batchSize) {
            this.commit();
        }
    }

    public abstract org.apache.tinkerpop.gremlin.structure.Vertex
                    constructHugeVertex(Vertex vertex);

    @Override
    public void close() {
        if (!this.vertexBatch.isEmpty()) {
            this.commit();
        }
        this.taskManager.waitFinished();
        this.taskManager.shutdown();
        LOG.info("End write back partition {}", this.partition());
    }

    private void commit() {
        this.taskManager.submitBatch(this.vertexBatch);
        LOG.debug("Write back {} vertices", this.vertexBatch.size());

        this.vertexBatch = new ArrayList<>();
    }
}
