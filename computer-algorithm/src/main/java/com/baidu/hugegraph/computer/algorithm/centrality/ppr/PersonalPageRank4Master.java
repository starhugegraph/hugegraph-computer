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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.master.MasterComputation;
import com.baidu.hugegraph.computer.core.master.MasterComputationContext;
import com.baidu.hugegraph.computer.core.master.MasterContext;
import com.baidu.hugegraph.util.Log;

/**
 * Personalized PageRank (PPR) is a variant of the PageRank algorithm that
 * calculates the importance of nodes in a graph from the perspective of a
 * specific node.
 *
 * For PPR, random jumps refer back to a given set of starting nodes.
 * This biases results toward, or personalizes for, the start node.
 * This bias & localization make PPR useful for highly targeted recommendations.
 */
public class PersonalPageRank4Master implements MasterComputation {

    private static final Logger LOG = Log.logger(PersonalPageRank4Master.class);

    protected static final String CONF_L1_NORM_DIFFERENCE_THRESHOLD_KEY =
                                  "ppr.l1DiffThreshold";
    protected static final double CONF_L1_DIFF_THRESHOLD_DEFAULT = 0.001D;

    protected static final String AGGR_L1_NORM_DIFFERENCE_KEY =
                                  "ppr.aggr_l1_norm_difference";
    protected static final String AGGR_COMULATIVE_PROBABILITY =
                                  "ppr.comulative_probability";

    private double l1DiffThreshold;

    private MasterComputationContext finalContext;

    @Override
    public void init(MasterContext context) {
        this.l1DiffThreshold = context.config().getDouble(
                               CONF_L1_NORM_DIFFERENCE_THRESHOLD_KEY,
                               CONF_L1_DIFF_THRESHOLD_DEFAULT);
        context.registerAggregator(AGGR_COMULATIVE_PROBABILITY,
                                   ValueType.DOUBLE,
                                   DoubleValueSumCombiner.class);
        context.registerAggregator(AGGR_L1_NORM_DIFFERENCE_KEY,
                                   ValueType.DOUBLE,
                                   DoubleValueSumCombiner.class);
    }

    @Override
    public boolean compute(MasterComputationContext context) {
        DoubleValue cumulativeProbability = context.aggregatedValue(
                                            AGGR_COMULATIVE_PROBABILITY);
        DoubleValue l1NormDifference = context.aggregatedValue(
                                       AGGR_L1_NORM_DIFFERENCE_KEY);

        StringBuilder sb = new StringBuilder();
        sb.append("[Superstep ").append(context.superstep()).append("]")
          .append(", cumulative probability = ").append(cumulativeProbability)
          .append(", l1 norm difference = ").append(l1NormDifference.value());
        LOG.info("PersonalPageRank running status: {}", sb);

        boolean l1Diff = l1NormDifference.value() > this.l1DiffThreshold;
        return context.superstep() <= 1 || l1Diff;
    }

    @Override
    public void afterSuperstep(MasterComputationContext context) {
        this.finalContext = context;
    }

    @Override
    public void output() {
        LOG.info("# Master output for :" + this.getClass().getSimpleName());
        DoubleValue cumulativeProbability = this.finalContext.aggregatedValue(
                                            AGGR_COMULATIVE_PROBABILITY);
        DoubleValue l1NormDifference = this.finalContext.aggregatedValue(
                                       AGGR_L1_NORM_DIFFERENCE_KEY);
        LOG.info("## {}, {}", cumulativeProbability, l1NormDifference);
    }

    @Override
    public void close(MasterContext context) {
        // pass
    }
}
