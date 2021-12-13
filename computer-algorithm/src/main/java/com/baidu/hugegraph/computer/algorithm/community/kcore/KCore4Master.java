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

package com.baidu.hugegraph.computer.algorithm.community.kcore;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.combiner.LongValueSumCombiner;
import com.baidu.hugegraph.computer.core.combiner.OverwriteCombiner;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.StringValue;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.master.MasterComputation;
import com.baidu.hugegraph.computer.core.master.MasterComputationContext;
import com.baidu.hugegraph.computer.core.master.MasterContext;
import com.baidu.hugegraph.util.Log;

public class KCore4Master implements MasterComputation {

    private static final Logger LOG = Log.logger(KCore4Master.class);

    public static final String AGGR_DELETE_VERTEX_NUM =
                               "kcore.delete_vertex_num";
    public static final String AGGR_ALGORITHM_STAGE =
                               "kcore.algorithm_stage";

    public static final String K_CORE = "K_CORE";
    public static final String WCC_0 = "WCC_0";
    public static final String WCC = "WCC";

    private String stage;

    @SuppressWarnings("unchecked")
    @Override
    public void init(MasterContext context) {
        this.stage = K_CORE;

        context.registerAggregator(AGGR_DELETE_VERTEX_NUM,
                                   ValueType.LONG,
                                   LongValueSumCombiner.class);
        context.registerAggregator(AGGR_ALGORITHM_STAGE,
                                   ValueType.STRING,
                                   OverwriteCombiner.class);
    }

    @Override
    public void beforeSuperstep(MasterComputationContext context) {
        LOG.info("Algorithm stage is {} in superstep {}",
                 this.stage, context.superstep());
    }

    @Override
    public boolean compute(MasterComputationContext context) {
        return true;
    }

    @Override
    public void afterSuperstep(MasterComputationContext context) {
        LongValue deletedVertexNumValue = context.aggregatedValue(
                                                  AGGR_DELETE_VERTEX_NUM);
        if (K_CORE.equals(this.stage)) {
            LOG.info("Delete {} vertex in superstep {}",
                     deletedVertexNumValue.value(), context.superstep());
        }

        if (K_CORE.equals(this.stage) && deletedVertexNumValue.value() == 0L &&
            context.superstep() > 0) {
            this.stage = WCC_0;
        } else if (WCC_0.equals(this.stage)) {
            this.stage = WCC;
        }
        context.aggregatedValue(AGGR_ALGORITHM_STAGE,
                                new StringValue(this.stage));
    }

    @Override
    public void close(MasterContext context) {
        // pass
    }
}
