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

import java.util.Iterator;
import java.util.Objects;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.combiner.ValueMinCombiner;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.StringValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;
import com.google.common.collect.Iterators;

public class KCore implements Computation<Id> {

    public static final String ALGORITHM_NAME = "kcore";
    public static final String OPTION_K = "kcore.k";
    public static final int K_DEFAULT_VALUE = 3;

    private final KCoreValue initValue = new KCoreValue();

    private int k;
    private Combiner<Id> wccCombiner;

    private String stage;
    private long currentStepDeleteVertexNum;

    @Override
    public String name() {
        return ALGORITHM_NAME;
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void init(Config config) {
        this.k = config.getInt(OPTION_K, K_DEFAULT_VALUE);
        this.wccCombiner = new ValueMinCombiner<>();
        this.currentStepDeleteVertexNum = 0L;
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
        this.currentStepDeleteVertexNum = 0L;
        StringValue algorithmStageValue =
                    context.aggregatedValue(KCore4Master.AGGR_ALGORITHM_STAGE);
        this.stage = algorithmStageValue.value();
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        KCoreValue value = this.initValue;
        value.core((Id) vertex.id().copy());
        vertex.value(value);

        if (vertex.numEdges() < this.k) {
            value.degree(0);
            currentStepDeleteVertexNum++;
            /*
             * TODO: send int type message at phase 1, it's different from id
             * type of phase 2 (wcc message), need support switch message type.
             */
            context.sendMessageToAllEdges(vertex, vertex.id());
            vertex.inactivate();
        } else {
            value.degree(vertex.numEdges());
            assert vertex.active();
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<Id> messages) {
        KCoreValue value = vertex.value();
        if (!value.active()) {
            vertex.inactivate();
            return;
        }

        switch (this.stage) {
            case KCore4Master.K_CORE:
                kcore(context, vertex, messages);
                break;
            case KCore4Master.WCC_0:
                wcc0(context, vertex);
                break;
            case KCore4Master.WCC:
                wcc(context, vertex, messages);
                break;
            default:
                throw new ComputerException("Stage error for: ", this.stage);
        }
    }

    private void kcore(ComputationContext context, Vertex vertex,
                       Iterator<Id> messages) {
        KCoreValue value = vertex.value();
        int deleted = Iterators.size(messages);
        if (value.decreaseDegree(deleted) < this.k) {
            // From active to inactive, delete self vertex
            value.degree(0);
            this.currentStepDeleteVertexNum++;

            context.sendMessageToAllEdges(vertex, vertex.id());
        }
    }

    private void wcc0(ComputationContext context, Vertex vertex) {
        context.sendMessageToAllEdgesIf(vertex, vertex.id(),
                                        (source, target) -> {
                                            return source.compareTo(target) < 0;
                                        });
        vertex.inactivate();
    }

    private void wcc(ComputationContext context, Vertex vertex,
                     Iterator<Id> messages) {
        if (!messages.hasNext()) {
            return;
        }
        KCoreValue value = vertex.value();
        Id min = Combiner.combineAll(this.wccCombiner, messages);
        assert Objects.nonNull(min);
        if (value.core().compareTo(min) > 0) {
            value.core(min);
            context.sendMessageToAllEdges(vertex, min);
        }
        vertex.inactivate();
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        context.aggregateValue(KCore4Master.AGGR_DELETE_VERTEX_NUM,
                               new LongValue(this.currentStepDeleteVertexNum));
    }
}
