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

import java.util.Set;

import org.junit.Test;

public class MhtTest {

    @Test
    public void test() {
        String config = "[" +
                        "    {" +
                        "        \"id\": \"A\"," +
                        "        \"label\": \"person\"" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"B\"," +
                        "        \"label\": \"person\"," +
                        "        \"property_filter\": \"$element.x > 3\"" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"C\"," +
                        "        \"label\": \"person\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"A\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element.x > " +
                        "3\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"D\"," +
                        "        \"label\": \"person\"," +
                        "        \"property_filter\": \"$element.x > 3\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"B\"," +
                        "                \"label\": \"knows\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"F\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element.x > " +
                        "3\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"C\"," +
                        "                \"label\": \"knows\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"E\"," +
                        "                \"label\": \"knows\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"E\"," +
                        "        \"label\": \"person\"" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"F\"," +
                        "        \"label\": \"person\"," +
                        "        \"property_filter\": \"$element.x > 3\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"B\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element.x > " +
                        "3\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"C\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element.x > " +
                        "3\"" +
                        "            }" +
                        "        ]" +
                        "    }" +
                        "]";

        QueryGraph graph = new QueryGraph(config);
        MinHeightTree tree = MinHeightTree.build(graph);

        Set<MinHeightTree.TreeNode> leaves;
        leaves = tree.nextLevelLeaves();
        leaves = tree.nextLevelLeaves();
        leaves = tree.nextLevelLeaves();
        leaves = tree.nextLevelLeaves();
    }
}
