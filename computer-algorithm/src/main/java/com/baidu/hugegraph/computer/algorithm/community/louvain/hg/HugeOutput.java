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

package com.baidu.hugegraph.computer.algorithm.community.louvain.hg;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.output.hg.task.TaskManager;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.structure.constant.WriteType;
import com.baidu.hugegraph.util.Log;

public class HugeOutput implements Closeable {

    private static final Logger LOG = Log.logger(HugeOutput.class);

    private final TaskManager taskManager;
    private List<com.baidu.hugegraph.structure.graph.Vertex> vertexBatch;
    private final int batchSize;

    public HugeOutput(Config config) {
        this.vertexBatch = new ArrayList<>();
        this.batchSize = config.get(ComputerOptions.OUTPUT_BATCH_SIZE);
        this.taskManager = new TaskManager(config);
        try {
            this.prepareSchema();
        } catch (Throwable e) {
            this.taskManager.shutdown();
            throw e;
        }
    }

    public HugeClient client() {
        return this.taskManager.client();
    }

    public String name() {
        return "louvain";
    }

    public void prepareSchema() {
        this.client().schema().propertyKey(this.name())
            .asText()
            .writeType(WriteType.OLAP_COMMON)
            .ifNotExist()
            .create();
    }

    public void write(Object vertexId, String value) {
        com.baidu.hugegraph.structure.graph.Vertex hugeVertex =
                new com.baidu.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertexId);
        hugeVertex.property(this.name(), value);

        this.vertexBatch.add(hugeVertex);
        if (this.vertexBatch.size() >= this.batchSize) {
            this.commit();
        }
    }

    @Override
    public void close() {
        if (!this.vertexBatch.isEmpty()) {
            this.commit();
        }
        this.taskManager.waitFinished();
        this.taskManager.shutdown();
    }

    private void commit() {
        this.taskManager.submitBatch(this.vertexBatch);
        LOG.info("Write back {} vertices", this.vertexBatch.size());

        this.vertexBatch = new ArrayList<>();
    }
}
