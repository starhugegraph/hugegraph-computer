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

package com.baidu.hugegraph.computer.algorithm.path.subgraph;

import java.util.Iterator;
import java.util.Set;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;

public class SubGraphMatch implements Computation<SubGraphMatchMessage> {

    public static final String SUBGRAPH_OPTION = "subgraph.query_graph_config";

    private MinHeightTree subgraphTree;
    private Set<MinHeightTree.TreeNode> leaves;

    @Override
    public String name() {
        return "subgraph_match";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void init(Config config) {
        String subgraphConfig = config.getString(SUBGRAPH_OPTION, "{}");
        this.subgraphTree = MinHeightTree.build(new QueryGraph(subgraphConfig));
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
        this.leaves = this.subgraphTree.nextLevelLeaves();
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {

    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<SubGraphMatchMessage> messages) {

    }
}
