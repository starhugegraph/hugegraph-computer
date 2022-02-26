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

package com.baidu.hugegraph.computer.algorithm.centrality.ppr;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.type.define.WriteType;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.id.IdGenerator;

public class PersonalPageRankListOutput extends HugeOutput {

    public static final int TOP_N = 100;

    @Override
    public void prepareSchema(HugeGraph graph) {
        graph.schema().propertyKey(this.name())
                     .asText()
                     .writeType(WriteType.OLAP_COMMON)
                     .ifNotExist()
                     .create();
    }

    @Override
    public HugeVertex constructHugeVertex(Vertex vertex) {
        GraphTransaction gtx = Whitebox.invoke(this.graph().getClass(),
                "graphTransaction", this.graph());
        HugeVertex hugeVertex = HugeVertex.create(gtx,
                IdGenerator.of(vertex.id().asObject()),
                VertexLabel.OLAP_VL);

        PersonalPageRankValue result = vertex.value();
        if (result.size() == 0) {
            return null;
        }

        result.remove(vertex.id());
        Edges edges = vertex.edges();
        for (Edge edge : edges) {
            Id targetV = edge.targetId();
            result.remove(targetV);
        }
        hugeVertex.property(this.name(), result.toString());
        return hugeVertex;
    }
}
