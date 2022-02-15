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

package com.baidu.hugegraph.computer.core.input.hg;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.type.HugeType;

public class HugeEdgeFetcher extends HugeElementFetcher<Edge>
                             implements EdgeFetcher<Edge> {

    public HugeEdgeFetcher(Config config, HugeGraph graph) {
        super(config, graph);
    }

    @Override
    public Iterator<Edge> fetch(InputSplit split) {
        Shard shard = toShard(split);
        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        query.scan(shard.start(), shard.end());
        String filterClassName = this.config().get(
                ComputerOptions.INPUT_FILTER_CLASS).getName();
        if (filterClassName.contains("DefaultInputFilter"))
            query.withProperties(false);
        else
            query.withProperties(true);
        return this.graph().edges(query);
    }

    @Override
    public void close() {
        // HugeClient was closed in HugeGraphFetcher
    }
}
