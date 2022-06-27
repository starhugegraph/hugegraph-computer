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

package com.baidu.hugegraph.computer.algorithm.community.cc;

import com.baidu.hugegraph.computer.core.graph.value.IdSet;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import java.io.IOException;
import javax.ws.rs.NotSupportedException;
import org.apache.commons.lang3.builder.ToStringBuilder;




/**
 * We should reuse triangle
 */
public class ClusteringCoefficientValue implements
                                        Value<ClusteringCoefficientValue> {
    private IdSet idSet;
    private IntValue count;
    private IntValue degree;
    private int shift;

    public ClusteringCoefficientValue() {
        this.idSet = new IdSet();
        this.count = new IntValue();
        this.degree = new IntValue();
    }

    public IdSet idSet() {
        return this.idSet;
    }

    public int count() {
        return this.count.value();
    }

    public void count(Integer count) {
        this.count.value(count);
    }

    public int degree() {
        return this.degree.value();
    }

    public void setDegree(Integer degree) {
        this.degree.value(degree);
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<ClusteringCoefficientValue> other) {
        throw new NotSupportedException();
    }

    @Override
    public Value<ClusteringCoefficientValue> copy() {
        ClusteringCoefficientValue ccValue = new ClusteringCoefficientValue();
        ccValue.idSet = (IdSet) this.idSet.copy();
        ccValue.count = this.count.copy();
        ccValue.degree = this.degree.copy();
        return ccValue;
    }

    @Override
    public void parse(byte[] buffer, int offset) {
        this.shift = 0;
        int position = offset;
        this.idSet.parse(buffer, position);
        this.shift += this.idSet.getShift();
        position += this.idSet.getShift();
        
        this.count.parse(buffer, position);
        this.shift += this.count.getShift();
        position += this.count.getShift();
        
        this.degree.parse(buffer, position);
        this.shift += this.degree.getShift();
        position += this.degree.getShift();
    }
    
    @Override
    public int getShift() {
        return this.shift;
    }
    
    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.idSet.read(in);
        this.count.read(in);
        this.degree.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.idSet.write(out);
        this.count.write(out);
        this.degree.write(out);
    }

    @Override
    public int compareTo(ClusteringCoefficientValue other) {
        throw new NotSupportedException();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                   .append("degree", this.degree)
                   .toString();
    }

    @Override
    public Object value() {
        throw new NotSupportedException();
    }

    @Override
    public String string() {
        int degree = this.idSet().size();
        if (degree <= 1) {
            return "0.0";
        } else {
            return String.valueOf(2 * this.count.floatValue() / degree / (degree - 1));
        }
    }
}
