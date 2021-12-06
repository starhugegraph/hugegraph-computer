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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.output.LimitedLogOutput;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;

public class SubGraphMatchTest extends AlgorithmTestBase {

    @Before
    public void init() {
        clearAll();
    }

    @After
    public void teardown() {
        clearAll();
    }

    @Test
    public void testSubGraphLabelMatch() throws InterruptedException {
        final String VERTEX_LABEL = "person";
        final String EDGE_LABEL = "knows";

        SchemaManager schema = client().schema();

        schema.vertexLabel(VERTEX_LABEL)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL)
              .sourceLabel(VERTEX_LABEL)
              .targetLabel(VERTEX_LABEL)
              .ifNotExist()
              .create();

        GraphManager graph = client().graph();
        Vertex vA = graph.addVertex(T.label, VERTEX_LABEL, T.id, "A");
        Vertex vB = graph.addVertex(T.label, VERTEX_LABEL, T.id, "B");
        Vertex vC = graph.addVertex(T.label, VERTEX_LABEL, T.id, "C");
        Vertex vD = graph.addVertex(T.label, VERTEX_LABEL, T.id, "D");
        Vertex vE = graph.addVertex(T.label, VERTEX_LABEL, T.id, "E");

        graph.addEdge(vA, EDGE_LABEL, vB);
        graph.addEdge(vA, EDGE_LABEL, vD);
        graph.addEdge(vA, EDGE_LABEL, vE);
        graph.addEdge(vB, EDGE_LABEL, vC);
        graph.addEdge(vC, EDGE_LABEL, vA);
        graph.addEdge(vC, EDGE_LABEL, vD);
        graph.addEdge(vD, EDGE_LABEL, vE);
        graph.addEdge(vE, EDGE_LABEL, vD);

        String config = "[" +
                        "    {" +
                        "        \"id\": \"A\"," +
                        "        \"label\": \"person\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"knows\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"E\"," +
                        "                \"label\": \"knows\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"C\"," +
                        "        \"label\": \"person\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"A\"," +
                        "                \"label\": \"knows\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"knows\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"E\"," +
                        "        \"label\": \"person\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"knows\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"D\"," +
                        "        \"label\": \"person\"" +
                        "    }" +
                        "]";

        runAlgorithm(SubGraphMatchParams.class.getName(),
                     SubGraphMatch.SUBGRAPH_OPTION, config,
                     ComputerOptions.OUTPUT_CLASS.name(),
                     LimitedLogOutput.class.getName());
    }

    @Test
    public void testSubMatchWithSameVertex() throws InterruptedException {
        final String LABEL_A = "A";
        final String LABEL_B = "B";
        final String LABEL_C = "C";
        final String LABEL_D = "D";
        final String EDGE_LABEL_A_B = "A_B";
        final String EDGE_LABEL_B_C = "B_C";
        final String EDGE_LABEL_C_D = "C_D";
        final String EDGE_LABEL_A_D = "A_D";
        final String EDGE_LABEL_B_D = "B_D";

        SchemaManager schema = client().schema();

        schema.vertexLabel(LABEL_A)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.vertexLabel(LABEL_B)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.vertexLabel(LABEL_C)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.vertexLabel(LABEL_D)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_A_B)
              .sourceLabel(LABEL_A)
              .targetLabel(LABEL_B)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_B_C)
              .sourceLabel(LABEL_B)
              .targetLabel(LABEL_C)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_C_D)
              .sourceLabel(LABEL_C)
              .targetLabel(LABEL_D)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_A_D)
              .sourceLabel(LABEL_A)
              .targetLabel(LABEL_D)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_B_D)
              .sourceLabel(LABEL_B)
              .targetLabel(LABEL_D)
              .ifNotExist()
              .create();

        GraphManager graph = client().graph();
        Vertex vA = graph.addVertex(T.label, LABEL_A, T.id, "A");
        Vertex vB1 = graph.addVertex(T.label, LABEL_B, T.id, "B1");
        Vertex vB2 = graph.addVertex(T.label, LABEL_B, T.id, "B2");
        Vertex vC1 = graph.addVertex(T.label, LABEL_C, T.id, "C2");
        Vertex vC2 = graph.addVertex(T.label, LABEL_C, T.id, "C1");
        Vertex vD = graph.addVertex(T.label, LABEL_D, T.id, "D");

        graph.addEdge(vA, EDGE_LABEL_A_B, vB1);
        graph.addEdge(vA, EDGE_LABEL_A_B, vB2);
        graph.addEdge(vA, EDGE_LABEL_A_D, vD);
        graph.addEdge(vB1, EDGE_LABEL_B_C, vC1);
        graph.addEdge(vB2, EDGE_LABEL_B_C, vC2);
        graph.addEdge(vB1, EDGE_LABEL_B_D, vD);
        graph.addEdge(vB2, EDGE_LABEL_B_D, vD);
        graph.addEdge(vC1, EDGE_LABEL_C_D, vD);
        graph.addEdge(vC2, EDGE_LABEL_C_D, vD);


        String config = "[" +
                        "    {" +
                        "        \"id\": \"A\"," +
                        "        \"label\": \"A\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"A_D\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"B\"," +
                        "                \"label\": \"A_B\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"B\"," +
                        "        \"label\": \"B\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"C\"," +
                        "                \"label\": \"B_C\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"B_D\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"C\"," +
                        "        \"label\": \"C\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"C_D\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"D\"," +
                        "        \"label\": \"D\"" +
                        "    }" +
                        "]";

        runAlgorithm(SubGraphMatchParams.class.getName(),
                     SubGraphMatch.SUBGRAPH_OPTION, config,
                     ComputerOptions.OUTPUT_CLASS.name(),
                     LimitedLogOutput.class.getName());
    }
}
