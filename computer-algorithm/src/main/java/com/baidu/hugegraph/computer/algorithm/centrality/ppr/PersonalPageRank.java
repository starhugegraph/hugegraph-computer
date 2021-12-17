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

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;

/**
 * In (original) PageRank, the damping factor is the probability of the surfer
 * continues browsing at each step. The surfer may also stop browsing and start
 * again from a random vertex.
 *
 * But in "Personalized PageRank", the surfer can only start browsing from a
 * given set of source vertices both at the beginning and after stopping.
 */
public class PersonalPageRank implements Computation<DoubleValue> {

    public static final String OPTION_ALPHA = "ppr.alpha";

    public static final double ALPHA_DEFAULT_VALUE = 0.85;
    public static final int RESULT_LIMIT = 64;

    private double alpha;
    private double cumulativeRank;

    private Aggregator<DoubleValue> diffAggr;
    private Aggregator<DoubleValue> cumulativeRankAggr;

    private Id sourceId;

    @Override
    public String name() {
        return "ppr";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void init(Config config) {
        this.alpha = config.getDouble(OPTION_ALPHA, ALPHA_DEFAULT_VALUE);
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
        // Get aggregator values for computation
        DoubleValue cumulativeRank = context.aggregatedValue(
                PersonalPageRank4Master.AGGR_COMULATIVE_PROBABILITY);

        /* Algorithm params */
        this.cumulativeRank = cumulativeRank.value();
        /* Update aggregator values */
        this.diffAggr = context.createAggregator(
                PersonalPageRank4Master.AGGR_L1_NORM_DIFFERENCE_KEY);
        this.cumulativeRankAggr = context.createAggregator(
                PersonalPageRank4Master.AGGR_COMULATIVE_PROBABILITY);
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        PersonalPageRankValue pprValue = new PersonalPageRankValue();
        vertex.value(pprValue);
        this.cumulativeRankAggr.aggregateValue(pprValue.contribValue());
        int degree = vertex.numEdges();

        if (degree > 0) {
            this.sourceId = vertex.id();
            pprValue.contribRank(pprValue.contribRank() / degree);
            context.sendMessageToAllEdges(vertex, pprValue.contribValue());
        } else {
            vertex.inactivate();
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<DoubleValue> messages) {
        DoubleValue message = Combiner.combineAll(context.combiner(), messages);
        double rankFromNeighbors = (message != null ? message.value() : 0);

        double initialRank = (vertex.id() == sourceId ? 1 : 0);
        double rank = rankFromNeighbors * this.alpha +
                      (1 - this.alpha) * initialRank;
        rank /= this.cumulativeRank;

        PersonalPageRankValue ppr = vertex.value();
        ppr.contribRank(rank);
        Edges edges = vertex.edges();
        int degree = edges.size();

        for (Edge edge : edges) {
            Id neighbor = edge.targetId();
            if (ppr.size() < RESULT_LIMIT || ppr.containsKey(neighbor)) {
                ppr.put(neighbor, new DoubleValue(rank));
            }
        }

        //vertex.value(ppr);
        this.diffAggr.aggregateValue(Math.abs(ppr.contribRank() - rank));
        this.cumulativeRankAggr.aggregateValue(rank);

        if (degree != 0) {
            context.sendMessageToAllEdges(vertex, new DoubleValue(rank /
                                                                  degree));
        }
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        context.aggregateValue(
                PersonalPageRank4Master.AGGR_COMULATIVE_PROBABILITY,
                this.cumulativeRankAggr.aggregatedValue());
        context.aggregateValue(
                PersonalPageRank4Master.AGGR_L1_NORM_DIFFERENCE_KEY,
                this.diffAggr.aggregatedValue());
    }
}
