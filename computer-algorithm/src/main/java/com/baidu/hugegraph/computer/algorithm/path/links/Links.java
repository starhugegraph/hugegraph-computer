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

package com.baidu.hugegraph.computer.algorithm.path.links;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerService;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;

public class Links implements Computation<LinksMessage> {

    public static final String OPTION_ANALYZE_CONFIG = "links.analyze_config";

    private LinksSpreadFilter filter;
    private int lastStep;

    @Override
    public String name() {
        return "olap_links";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void init(Config config) {
        this.lastStep = config.getInt(ComputerOptions.BSP_MAX_SUPER_STEP.name(), 1) - 1;
        LOG.info("links last step: {}", this.lastStep);
        String describe = config.getString(OPTION_ANALYZE_CONFIG, "{}");
        try {
            this.filter = new LinksSpreadFilter(describe);
        } catch (ComputerException e) {
            WorkerService.setThrowable(e);
        }
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new LinksValue());
        if (vertex.edges().size() == 0 ||
            !this.filter.isStartVertexes(vertex)) {
            return;
        }

        LinksMessage message = new LinksMessage();
        if (this.isEndVertexAndSaveValue(vertex, message)) {
            return;
        }
        message.addVertex(vertex.id());
        for (Edge edge : vertex.edges()) {
            LinksMessage copyMessage = message.copy();
            if (this.isEndEdgeAndSaveValue(vertex, edge, copyMessage)) {
                continue;
            }
            if (this.filter.isEdgeCanSpread0(edge)) {
                copyMessage.addEdge(edge.id());
                copyMessage.walkEdgeProp(edge.properties());
                context.sendMessage(edge.targetId(), copyMessage);
            }
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<LinksMessage> messages) {
        boolean half = true;
        while (messages.hasNext()) {
            half = false;
            LinksMessage message = messages.next();
            if (this.isEndStepAndSaveValue(vertex, message, context)) {
                continue;
            }
            if (this.isEndVertexAndSaveValue(vertex, message)) {
                continue;
            }
            message.addVertex(vertex.id());
            for (Edge edge : vertex.edges()) {
                LinksMessage copyMessage = message.copy();
                if (this.isEndEdgeAndSaveValue(vertex, edge, copyMessage)) {
                    continue;
                }
                if (this.filter.isEdgeCanSpread(edge,
                                                copyMessage.walkEdgeProp())) {
                    copyMessage.addEdge(edge.id());
                    copyMessage.walkEdgeProp(edge.properties());
                    context.sendMessage(edge.targetId(), copyMessage);
                }
            }
        }
        if (half) {
            vertex.inactivate();
        }
    }

    private boolean isEndStepAndSaveValue(Vertex vertex,
                                          LinksMessage message,
                                          ComputationContext context) {
        if (context.superstep() >= this.lastStep || vertex.numEdges() == 0) {
            message.addVertex(vertex.id());
            LinksValue value = vertex.value();
            value.addValue(message.pathVertexes(), message.pathEdge());
            return true;
        }
        return false;
    }

    private boolean isEndVertexAndSaveValue(Vertex vertex,
                                            LinksMessage message) {
        if (this.filter.isEndVertex(vertex)) {
            message.addVertex(vertex.id());
            LinksValue value = vertex.value();
            value.addValue(message.pathVertexes(), message.pathEdge());
            return true;
        }
        return false;
    }

    private boolean isEndEdgeAndSaveValue(Vertex vertex, Edge edge,
                                          LinksMessage message) {
        if (this.filter.isEndEdge(edge)) {
            message.addVertex(edge.targetId());
            message.addEdge(edge.id());
            LinksValue value = vertex.value();
            value.addValue(message.pathVertexes(),
                           message.pathEdge());
            return true;
        }
        return false;
    }
}
