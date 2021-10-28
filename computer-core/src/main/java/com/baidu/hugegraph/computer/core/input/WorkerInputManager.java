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

package com.baidu.hugegraph.computer.core.input;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.vertex.DefaultVertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.worker.load.LoadService;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.util.Log;

public class WorkerInputManager implements Manager {

    public static final String NAME = "worker_input";
    private static final Logger LOG = Log.logger(WorkerInputManager.class);

    /*
     * Fetch vertices and edges from the data source and convert them
     * to computer-vertices and computer-edges
     */
    private final LoadService loadService;
    private final GraphFactory graphFactory;
    /*
     * Send vertex/edge or message to target worker
     */
    private final MessageSendManager sendManager;
    private Config config;

    public WorkerInputManager(ComputerContext context,
                              MessageSendManager sendManager) {
        this.loadService = new LoadService(context);
        this.sendManager = sendManager;
        this.graphFactory = context.graphFactory();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.loadService.init();
        this.sendManager.init(config);
        this.config = config;
    }

    @Override
    public void close(Config config) {
        this.loadService.close();
        this.sendManager.close(config);
    }

    public void service(InputSplitRpcService rpcService) {
        this.loadService.rpcService(rpcService);
    }

    /**
     * TODO: Load vertices and edges parallel.
     * When this method finish, it means that all vertices and edges are sent,
     * but there is no guarantee that all of them has been received.
     */
    public void loadGraph() {
        StopWatch watcher = new StopWatch();
        watcher.start();
        this.sendManager.startSend(MessageType.VERTEX);
        Iterator<Vertex> iterator = this.loadService.createIteratorFromVertex();
        long time = 0;
        StopWatch watcher2 = new StopWatch();
        while (true) {
            watcher2.reset();
            watcher2.start();
            boolean b = iterator.hasNext();
            if (!b) {
                watcher2.stop();
                time += watcher2.getTime(TimeUnit.MILLISECONDS);
                break;
            }
            Vertex vertex = iterator.next();
            watcher2.stop();
            time += watcher2.getTime(TimeUnit.MILLISECONDS);
            this.sendManager.sendVertex(vertex);
        }
        LOG.info("load vertx cost:{}", time);
        this.sendManager.finishSend(MessageType.VERTEX);
        watcher.stop();
        LOG.info("input vertex cost:{}",
                 watcher.getTime(TimeUnit.MILLISECONDS));
        watcher.reset();
        watcher.start();
        this.sendManager.startSend(MessageType.EDGE);
        time = 0;
        iterator = this.loadService.createIteratorFromEdge();
        StopWatch watcher3 = new StopWatch();
        while (true) {
            watcher3.reset();
            watcher3.start();
            boolean b = iterator.hasNext();
            if (!b) {
                watcher3.stop();
                time += watcher3.getTime(TimeUnit.MILLISECONDS);
                break;
            }
            Vertex vertex = iterator.next();
            watcher3.stop();
            time += watcher3.getTime(TimeUnit.MILLISECONDS);
            this.sendManager.sendEdge(vertex);
            //inverse edge here
            if (!this.config.get(
                     ComputerOptions.VERTEX_WITH_EDGES_BOTHDIRECTION)) {
                continue;
            }
            for (Edge edge:vertex.edges()) {
                Id targetId = edge.targetId();
                Id sourceId = vertex.id();

                Vertex vertexInv = new DefaultVertex(graphFactory,
                                                  targetId, null);
                Edge edgeInv = graphFactory.
                               createEdge(edge.label(), edge.name(), sourceId
                );
                Properties properties = edge.properties();
                properties.put("inv", new BooleanValue(true));
                edgeInv.properties(properties);
                vertexInv.addEdge(edgeInv);
                this.sendManager.sendEdge(vertexInv);
           }
        }
        LOG.info("load edge cost:{}", time);
        this.sendManager.finishSend(MessageType.EDGE);
        watcher.stop();
        LOG.info("input edge cost:{}", watcher.getTime(TimeUnit.MILLISECONDS));
    }
}
