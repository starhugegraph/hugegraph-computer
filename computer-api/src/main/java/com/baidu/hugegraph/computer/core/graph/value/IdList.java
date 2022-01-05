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

package com.baidu.hugegraph.computer.core.graph.value;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class IdList extends ListValue<Id> {

    public IdList() {
        super(ValueType.ID);
    }

    @Override
    public ValueType valueType() {
        return ValueType.ID_LIST;
    }

    @Override
    public void parse(byte[] buffer, int offset) {
        this.parse(buffer, offset, false);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.read(in, false);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.write(out, false);
    }

    @Override
    public IdList copy() {
        IdList values = new IdList();
        for (Id value : this.values()) {
            values.add((Id) value.copy());
        }
        return values;
    }
}
