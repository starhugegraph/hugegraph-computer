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

package com.baidu.hugegraph.computer.core.output.hg.task;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.output.hg.metrics.LoadMetrics;
import com.baidu.hugegraph.computer.core.output.hg.metrics.LoadSummary;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.testutil.Whitebox;

public abstract class InsertTask implements Runnable {

    public static final String[] UNACCEPTABLE_MESSAGES = {
            // org.apache.http.conn.HttpHostConnectException
            "Connection refused",
            "The server is being shutting down",
            "not allowed to insert, because already exist a vertex " +
            "with same id and different label"
    };

    protected Config config;
    private HugeGraph graph;
    protected final List<Vertex> batch;
    private LoadSummary summary;

    public InsertTask(Config config, HugeGraph graph,
                      List<Vertex> batch, LoadSummary loadSummary) {
        this.config = config;
        this.graph = graph;
        this.batch = batch;
        this.summary = loadSummary;
    }

    public LoadSummary summary() {
        return this.summary;
    }

    public LoadMetrics metrics() {
        return this.summary().metrics();
    }

    protected void plusLoadSuccess(int count) {
        LoadMetrics metrics = this.summary().metrics();
        metrics.plusInsertSuccess(count);
        this.summary().plusLoaded(count);
    }

    protected void increaseLoadSuccess() {
        this.plusLoadSuccess(1);
    }

    protected void insertBatch(List<Vertex> vertices) {

        GraphTransaction gtx = Whitebox.invoke(this.graph.getClass(),
                                               "graphTransaction", this.graph);
        for (Vertex vertex : vertices) {
            gtx.addVertex((HugeVertex) vertex);
        }
        this.graph.tx().commit();
    }
}
