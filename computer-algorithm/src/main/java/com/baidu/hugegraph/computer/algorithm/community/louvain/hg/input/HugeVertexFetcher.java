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

package com.baidu.hugegraph.computer.algorithm.community.louvain.hg.input;

import java.util.Iterator;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.pd.grpc.Metapb;
//import com.baidu.hugegraph.structure.graph.Vertex;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import com.baidu.hugegraph.type.HugeType;

public class HugeVertexFetcher extends HugeElementFetcher<Vertex> {

    public HugeVertexFetcher(Config config, HugeGraph graph) {
        super(config, graph);
    }

    @Override
    protected Iterator<Vertex> fetch(Metapb.Partition shard) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.scan(String.valueOf(shard.getStartKey()),
                String.valueOf(shard.getEndKey()));
        return this.graph().vertices(query);
    }
}
