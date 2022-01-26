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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.NotSupportedException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.google.common.collect.ImmutableMap;

public class PersonalPageRankValue implements Value<PersonalPageRankValue> {

    private final GraphFactory graphFactory;

    private final DoubleValue contribRank;
    private Map<Id, DoubleValue> map;
    private int shift = 0;

    public PersonalPageRankValue() {
        this.graphFactory = ComputerContext.instance().graphFactory();
        this.contribRank = new DoubleValue(1.0);
        this.map = this.graphFactory.createMap();
    }

    public PersonalPageRankValue(boolean source) {
        this.graphFactory = ComputerContext.instance().graphFactory();
        this.contribRank = source ? new DoubleValue(1.0) : new DoubleValue(0.0);
        this.map = source ? this.graphFactory.createMap() : ImmutableMap.of();
    }

    public DoubleValue get(Id id) {
        return this.map.get(id);
    }

    public void contribRank(Double rank) {
        this.contribRank.value(rank);
    }

    public double contribRank() {
        return this.contribRank.value();
    }

    public DoubleValue contribValue() {
        return this.contribRank;
    }

    public void put(Id id, DoubleValue value) {
        if (this.map.size() < PersonalPageRank.RESULT_LIMIT ||
            this.map.containsKey(id)) {
            this.map.put(id, value);
        }
    }

    public void remove(Id id) {
        this.map.remove(id);
    }

    public Set<Map.Entry<Id, DoubleValue>> entrySet() {
        return this.map.entrySet();
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<PersonalPageRankValue> value) {
        throw new NotSupportedException();
    }

    @Override
    public Value<PersonalPageRankValue> copy() {
        throw new NotSupportedException();
    }

    @Override
    public Object value() {
        throw new NotSupportedException();
    }

    @Override
    public void parse(byte[] buffer, int offset) {
        this.shift = 0;
        int position = offset;

        this.contribRank.parse(buffer, offset);
        position += this.contribRank.getShift();
        this.shift += this.contribRank.getShift();
        
        int size = buffer[position];
        position++;
        this.shift++;
    
        this.map = this.graphFactory.createMap();
        for (int i = 0; i < size; i++) {
            Id id = this.graphFactory.createId();
            id.parse(buffer, position);
            this.shift += id.getShift();
            position += id.getShift();
            
            Value<?> value = this.graphFactory.createValue(ValueType.DOUBLE);
            value.parse(buffer, position);
            this.shift += value.getShift();
            position += value.getShift();
            this.map.put(id, (DoubleValue) value);
        }
    }

    @Override
    public int getShift() {
        return this.shift;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.contribRank.read(in);
        int size = in.readInt();
        this.map = this.graphFactory.createMap();
        for (int i = 0; i < size; i++) {
            Id id = this.graphFactory.createId();
            id.read(in);
            Value<?> value = this.graphFactory.createValue(ValueType.DOUBLE);
            value.read(in);
            this.map.put(id, (DoubleValue) value);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.contribRank.write(out);
        out.writeInt(this.map.size());
        for (Map.Entry<Id, DoubleValue> entry : this.map.entrySet()) {
            Id id = entry.getKey();
            DoubleValue value = entry.getValue();
            id.write(out);
            value.write(out);
        }
    }

    @Override
    public int compareTo(PersonalPageRankValue o) {
        throw new NotSupportedException();
    }

    public int size() {
        return this.map.size();
    }

    public boolean containsKey(Id id) {
        return this.map.containsKey(id);
    }

    @Override
    public String string() {
        return String.valueOf(this.map);
    }

    @Override
    public String toString() {
        return "RankMap = " + string();
    }
}
