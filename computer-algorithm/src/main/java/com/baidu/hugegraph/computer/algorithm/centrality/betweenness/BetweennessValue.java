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

import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.IdSet;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import java.io.IOException;


public class BetweennessValue implements Value<BetweennessValue> {

    private final DoubleValue betweenness;
    private final IdSet arrivedVertices;
    private int shift = 0;

    public BetweennessValue() {
        this(0.0D);
    }

    public BetweennessValue(double betweenness) {
        this.betweenness = new DoubleValue(betweenness);
        this.arrivedVertices = new IdSet();
    }

    public DoubleValue betweenness() {
        return this.betweenness;
    }

    public IdSet arrivedVertices() {
        return this.arrivedVertices;
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<BetweennessValue> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value<BetweennessValue> copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object value() {
        throw new UnsupportedOperationException();
    }

    @Override   
    public void parse(byte[] buffer, int offset) {
        int position = offset;
        this.shift = 0;

        this.betweenness.parse(buffer, position);
        position += this.betweenness.getShift();
        this.shift += this.betweenness.getShift();
        
        this.arrivedVertices.parse(buffer, position);
        this.shift += this.arrivedVertices.getShift();
    }
    
    @Override
    public int getShift() {
        return this.shift;
    }
    
    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.betweenness.read(in);
        this.arrivedVertices.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.betweenness.write(out);
        this.arrivedVertices.write(out);
    }

    @Override
    public int compareTo(BetweennessValue o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String string() {
        return this.betweenness.toString();
    }
}
