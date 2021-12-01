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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class SubGraphMatch implements Computation<SubGraphMatchMessage> {

    public static final String SUBGRAPH_OPTION = "subgraph.query_graph_config";

    private MinHeightTree subgraphTree;

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
        String subgraphConfig = config.getString(SUBGRAPH_OPTION, null);
        if (subgraphConfig == null) {
            throw new ComputerException("Config %s must not be null",
                                        SUBGRAPH_OPTION);
        }
        this.subgraphTree = MinHeightTree.build(new QueryGraph(subgraphConfig));
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new SubGraphMatchValue());

        Set<MinHeightTree.TreeNode> leaves = this.subgraphTree.leaves();
        for (MinHeightTree.TreeNode leaf : leaves) {
            if (!leaf.match(vertex)) {
                break;
            }
            SubGraphMatchMessage message = new SubGraphMatchMessage();
            message.merge(new MutablePair<>(leaf.nodeId(), vertex.id()));
            this.sendMessage(context, leaf, vertex, message);
        }

        vertex.inactivate();
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<SubGraphMatchMessage> messages) {
        if (context.superstep() >= this.subgraphTree.treeHeight()) {
            vertex.inactivate();
            return;
        }

        SubGraphMatchValue value = vertex.value();
        while (messages.hasNext()) {
            SubGraphMatchMessage message = messages.next();

            MinHeightTree.TreeNode lastNode =
                          this.subgraphTree.findNodeById(
                                            message.lastNode().getLeft());
            MinHeightTree.TreeNode parent = lastNode.parent();
            if (!parent.match(vertex)) {
                continue;
            }

            message.merge(new MutablePair<>(parent.nodeId(), vertex.id()));
            if (parent == this.subgraphTree.root()) {
                // Filter out invalid match path
                List<Pair<Integer, Id>> path = message.matchPath();
                Set<Id> ids = path.stream().map(Pair::getRight)
                                           .collect(Collectors.toSet());
                if (ids.size() != path.size()) {
                    continue;
                }

                List<Integer> pathIds = path.stream()
                                            .map(Pair::getLeft)
                                            .collect(Collectors.toList());
                if (this.subgraphTree.matchRootPath(pathIds)) {
                    value.addMp(path);
                }
            } else {
                this.sendMessage(context, parent, vertex, message);
            }
        }

        if (context.superstep() == this.subgraphTree.treeHeight() - 1 &&
            this.subgraphTree.matchRoot(vertex)) {
            this.setValueRes(value);
        }

        vertex.inactivate();
    }

    private void sendMessage(ComputationContext context,
                             MinHeightTree.TreeNode node,
                             Vertex vertex, SubGraphMatchMessage message) {
        for (Edge edge : vertex.edges()) {
            if (node.isInToParent() == edge.isInverse() ||
                !node.match(edge)) {
                continue;
            }
            context.sendMessage(edge.targetId(), message);
        }
    }

    private void setValueRes(SubGraphMatchValue value) {
        List<List<Pair<Integer, Id>>> mp = value.mp();
        List<List<Integer>> paths = this.subgraphTree.paths();

        List<List<List<Id>>> group = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            group.add(new ArrayList<>());
        }
        for (List<Pair<Integer, Id>> mpItem : mp) {
            for (int i = 0; i < paths.size(); i++) {
                List<Integer> path = paths.get(i);
                List<Integer> mpItemPath = mpItem.stream()
                                                 .map(Pair::getLeft)
                                                 .collect(Collectors.toList());
                if (this.pathMatch(path, mpItemPath)) {
                    List<Id> idPath = mpItem.stream()
                                            .map(Pair::getRight)
                                            .collect(Collectors.toList());
                    group.get(i).add(idPath);
                }
            }
        }
        value.clearMp();

        for (List<List<Id>> groupItem : group) {
            if (groupItem.size() == 0) {
                return;
            }
        }

        // Cartesian Product
        cartesianProduct(group, 0, new HashSet<>(), value);
    }

    private boolean pathMatch(List<Integer> path, List<Integer> other) {
        if (path.size() != other.size()) {
            return false;
        }
        for (int i = 0; i < path.size(); i++) {
            if (!path.get(i).equals(other.get(i))) {
                return false;
            }
        }
        return true;
    }

    private void cartesianProduct(List<List<List<Id>>> group, int index,
                                  Set<Id> res, SubGraphMatchValue value) {
        List<List<Id>> groupItem = group.get(index);
        for (List<Id> idList : groupItem) {
            List<Id> notExist = new ArrayList<>();
            for (Id id : idList) {
                if (!res.contains(id)) {
                    notExist.add(id);
                    res.add(id);
                }
            }

            if (index == group.size() - 1) {
                if (this.subgraphTree.graphVertexSize() == res.size()) {
                    IdList ids = new IdList();
                    ids.addAll(res);
                    value.addRes(ids);
                }
            } else {
                cartesianProduct(group, index + 1, res, value);
            }

            for (Id id : notExist) {
                res.remove(id);
            }
        }
    }
}
