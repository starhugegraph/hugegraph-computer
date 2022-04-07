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

package com.baidu.hugegraph.computer.algorithm.community.lpa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Comparator;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.commons.lang3.mutable.MutableInt;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class Lpa implements Computation<Id> {

    @Override
    public String name() {
        return "olap_lpa";
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        Id value = vertex.id();
        vertex.value(value);
        context.sendMessageToAllEdges(vertex, value);
        vertex.inactivate();
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<Id> messages) {
        Id label = this.voteLabel(messages);
        vertex.value(label);
        context.sendMessageToAllEdges(vertex, label);
        vertex.inactivate();
    }

    private Id voteLabel(Iterator<Id> messages) {
        // Calculate label frequency
        // Map<Id, MutableInt> labels = new HashMap<>();
        // assert messages.hasNext();
        // while (messages.hasNext()) {
        //     Id label = messages.next();
        //     MutableInt labelCount = labels.get(label);
        //     if (labelCount != null) {
        //         labelCount.increment();
        //     } else {
        //         labels.put(label, new MutableInt(1));
        //     }
        // }

        // use array sort instead of hashmap
        List<Id> lableArray = new ArrayList<>();
        while (messages.hasNext()) {
            lableArray.add(messages.next());
        }
        lableArray.sort(Comparator.comparing(Id -> Id));

        int maxCount = 0;
        int labelCount = 0;
        Id lastLabel = null;
        Id minLabel = null;
        for (Id label : lableArray) {
            if (label.compareTo(lastLabel) != 0) {
                if (labelCount > maxCount) {
                    maxCount = labelCount;
                    minLabel = lastLabel;
                }
                labelCount = 0;
                lastLabel = label;
            }
            labelCount += 1;
        }
        if (labelCount > maxCount) {
            maxCount = labelCount;
            minLabel = lastLabel;
        } 
        return minLabel;

        // Calculate the labels with maximum frequency and select min label
        // int maxFreq = 0;
        // Id resultLable = null;
        // for (Map.Entry<Id, MutableInt> e : labels.entrySet()) {
        //     int value = e.getValue().intValue();
        //     if (value > maxFreq) {
        //         maxFreq = value;
        //         resultLable = e.getKey();
        //     }
        //     if (value == maxFreq && e.getKey().compareTo(resultLable)<0) {
        //         resultLable = e.getKey();
        //     }
        // }

        // return resultLable;
    }
}
