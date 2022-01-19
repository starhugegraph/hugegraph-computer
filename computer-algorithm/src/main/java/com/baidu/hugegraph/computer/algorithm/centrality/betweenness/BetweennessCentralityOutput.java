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

package com.baidu.hugegraph.computer.algorithm.centrality.betweenness;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.WriteType;
import com.baidu.hugegraph.util.Log;

public class BetweennessCentralityOutput extends HugeOutput {

    private static final Logger LOG =
            Log.logger(BetweennessCentralityOutput.class);

    @Override
    public void prepareSchema() {
        this.graph().schema().propertyKey(this.name())
                             .asDouble()
                             .writeType(WriteType.OLAP_RANGE)
                             .ifNotExist()
                             .create();
    }

    @Override
    public HugeVertex constructHugeVertex(Vertex vertex) {
        HugeVertex hugeVertex = new HugeVertex(
                this.graph(), IdGenerator.of(vertex.id().asObject()),
                this.graph().vertexLabel(vertex.label()));
        BetweennessValue localValue = vertex.value();
        double centrality = localValue.betweenness().value();
        hugeVertex.property(this.name(), centrality);
        LOG.info("The betweenness centrality of vertex {} is {}",
                 vertex, centrality);
        return hugeVertex;
    }
}
