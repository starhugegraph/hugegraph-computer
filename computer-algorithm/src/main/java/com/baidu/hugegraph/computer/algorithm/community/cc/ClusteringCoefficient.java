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

package com.baidu.hugegraph.computer.algorithm.community.cc;


import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.partition.Partitioner;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdSet;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



/**
 * ClusteringCoefficient(CC) algorithm could calculate local & the whole graph:
 * 1. local cc: get triangles & degree for current vertex, calculate them
 * 2. whole cc have 2 ways to get the result: (NOT SUPPORTED NOW)
 *    - sum all open & closed triangles in graph, and calculate the result
 *    - sum all local cc for each vertex, and use avg as the whole graph result
 *
 * And we have 2 ways to count local cc:
 * 1. if we already saved the triangles in each vertex, we can calculate only
 *    in superstep0/compute0 to get the result
 * 2. if we want recount the triangles result, we can choose:
 *    - copy code from TriangleCount, then add extra logic
 *    - reuse code in TriangleCount (need solve compatible problem - TODO)
 *
 *  The formula of local CC is: C(v) = 2T / Dv(Dv - 1)
 *  v represents one vertex, T represents the triangles of current vertex,
 *  D represents the degree of current vertex
 */
public class ClusteringCoefficient implements Computation<IdList> {
    private Partitioner partitioner;
    private Map<Id, IdList> messageStorage;
    private int superedgeThreshold;

    @Override
    public String name() {
        return "olap_clustering_coefficient";
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void init(Config config) {
        // Reuse triangle count later
        this.partitioner = config.createObject(
                        ComputerOptions.WORKER_PARTITIONER);
        this.partitioner.init(config);
        this.messageStorage = new HashMap();
        this.superedgeThreshold = 
            config.get(ComputerOptions.MIN_EDGES_USE_SUPEREDGE_CACHE);
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        ClusteringCoefficientValue value = new ClusteringCoefficientValue();
        IdSet allNeighbors = value.idSet();

        IdList neighbors = new IdList();
        for (Edge edge : vertex.edges()) {
            Id targetId = edge.targetId();
            int compareResult = targetId.compareTo(vertex.id());
            if (compareResult != 0) {
                // Collect neighbors of id less than self from all neighbors
                if (compareResult < 0 && !allNeighbors.contains(targetId)) {
                    neighbors.add(targetId);
                }
                // Collect all neighbors, include of incoming and outgoing
                allNeighbors.add(targetId);
            }
        }

        Map<Integer, Id> partitionMinIdMap = this.findMinId(allNeighbors);

        // Send all neighbors of id less than self to neighbors
        for (Id targetId : allNeighbors.values()) {
            if (vertex.numEdges() < this.superedgeThreshold) {
                context.sendMessage(targetId, neighbors);
            }
            else {
                //new branch, try to save some message to send
                int partitionId = this.partitioner.partitionId(targetId);
                Id minId = partitionMinIdMap.get(partitionId);

                if (targetId.equals(minId)) {
                    byte[] flagByte = new byte[1];
                    flagByte[0] = 1;
                    Id flagId = new BytesId(IdType.FLAG, flagByte);    
                    neighbors.add(0, flagId);
                    context.sendMessage(targetId, neighbors);
                } 
                else {
                    IdList mapper = new IdList();
                    byte[] flagByte = new byte[1];
                    Id flagId = new BytesId(IdType.FLAG, flagByte);
                    mapper.add(flagId);
                    mapper.add(minId);
                    context.sendMessage(targetId, mapper);
                }
            }
        }
        vertex.value(value);
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        Integer count = this.triangleCount(context, vertex, messages);
        if (count != null) {
            ((ClusteringCoefficientValue) vertex.value()).count(count);
            vertex.inactivate();
        }
    }

    private IdList cacheOrHit(Id vertexId, IdList list) {
        if (list.size() == 0) {
            this.messageStorage.put(vertexId, list);
            return list;
        }

        Id id0 = list.get(0);
        IdType type0 = id0.idType();
        if (type0 != IdType.FLAG) {
            return list;
        }

        if (id0.toString().equals("true")) {
            //save
            this.messageStorage.put(vertexId, list);
            return list;
        }
        else {
            //map
            Id key = list.get(1);
            IdList listStored = this.messageStorage.get(key);
            return listStored;
        }
    }

    private Map<Integer, Id> findMinId(IdSet allNeighbors) {
        Map<Integer, Id> partitionMinIdMap = new HashMap();

        for (Id id : allNeighbors.values()) {
            int partitionId = this.partitioner.partitionId(id);
            if (!partitionMinIdMap.containsKey(partitionId)) {
                partitionMinIdMap.put(partitionId, id);
            }
            else {
                Id minId = partitionMinIdMap.get(partitionId);
                int status = id.compareTo(minId);
                if (status < 0) {
                    partitionMinIdMap.put(partitionId, id);
                }
            }
        }
        return partitionMinIdMap;
    }

    private Integer triangleCount(ComputationContext context, Vertex vertex,
                                  Iterator<IdList> messages) {
        IdSet allNeighbors = ((ClusteringCoefficientValue) vertex.value())
                                                                 .idSet();

        if (context.superstep() == 1) {
            int count = 0;

            while (messages.hasNext()) {
                IdList message = messages.next();
                IdList twoDegreeNeighbors = 
                       this.cacheOrHit(vertex.id(), message);
                if (twoDegreeNeighbors != null) {
                    for (Id twoDegreeNeighbor : twoDegreeNeighbors.values()) {
                        if (allNeighbors.contains(twoDegreeNeighbor)) {
                            count++;
                        }
                    }
                } 
            }

            return count;
        }
        return null;
    }
}