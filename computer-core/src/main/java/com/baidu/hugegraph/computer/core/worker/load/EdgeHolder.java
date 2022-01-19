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

package com.baidu.hugegraph.computer.core.worker.load;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.input.Holder;
import com.baidu.hugegraph.computer.core.input.HugeConverter;
import com.baidu.hugegraph.computer.core.input.InputFilter;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;

public class EdgeHolder implements Holder<Edge> {

    private com.baidu.hugegraph.structure.graph.Edge clientEdge;
    private com.baidu.hugegraph.structure.HugeEdge serverEdge;

    public EdgeHolder(Object object) {
        if (object instanceof com.baidu.hugegraph.structure.graph.Edge) {
            this.clientEdge =
                    (com.baidu.hugegraph.structure.graph.Edge) object;
        } else if (object instanceof com.baidu.hugegraph.structure.HugeEdge) {
            this.serverEdge =
                    (com.baidu.hugegraph.structure.HugeEdge) object;
        } else {
            throw new ComputerException("Invalid edge type %s",
                                        object.getClass());
        }
    }

    public Edge convert(InputFilter inputFilter, GraphFactory graphFactory) {
        inputFilter.filter(this);
        if (this.clientEdge != null) {
            Id targetId = HugeConverter.convertId(this.clientEdge.targetId());
            Properties properties = HugeConverter.convertProperties(
                    this.clientEdge.properties());
            properties.put("inv", new BooleanValue(false));
            Edge computerEdge = graphFactory.createEdge(
                    this.clientEdge.label(), this.clientEdge.name(), targetId);
            computerEdge.id(HugeConverter.convertId(this.clientEdge.id()));
            computerEdge.label(this.clientEdge.label());
            computerEdge.properties(properties);
            return computerEdge;
        } else {
            Id targetId = HugeConverter.convertId(
                    this.serverEdge.targetVertex().id().asObject());
            Properties properties = HugeConverter.convertProperties(
                    properties(this.serverEdge));
            properties.put("inv", new BooleanValue(false));
            Edge computerEdge = graphFactory.createEdge(
                    this.serverEdge.label(), this.serverEdge.name(), targetId);
            computerEdge.id(HugeConverter.convertId(
                    this.serverEdge.id().asObject()));
            computerEdge.label(this.serverEdge.label());
            computerEdge.properties(properties);
            return computerEdge;
        }
    }

    @Override
    public void clearProperties() {
        if (this.clientEdge != null) {
            this.clientEdge.properties().clear();
        } else {
            this.serverEdge.resetProperties();
        }
    }

    public Object sourceId() {
        if (this.clientEdge != null) {
            return this.clientEdge.sourceId();
        } else {
            return this.serverEdge.sourceVertex().id().asObject();
        }
    }

    private static Map<String, Object> properties(
            org.apache.tinkerpop.gremlin.structure.Element elem) {
        Map<String, Object> properties = new HashMap<>();
        for (HugeProperty p : ((HugeElement) elem).getProperties().values()) {
            properties.put(p.key(), p.value());
        }
        return properties;
    }
}
