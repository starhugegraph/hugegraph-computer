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
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.input.Holder;
import com.baidu.hugegraph.computer.core.input.HugeConverter;
import com.baidu.hugegraph.computer.core.input.InputFilter;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;

public class VertexHolder implements Holder<Vertex> {

    private com.baidu.hugegraph.structure.graph.Vertex clientVertex;
    private com.baidu.hugegraph.structure.HugeVertex serverVertex;

    public VertexHolder(Object object) {
        if (object instanceof com.baidu.hugegraph.structure.graph.Vertex) {
            this.clientVertex =
                    (com.baidu.hugegraph.structure.graph.Vertex) object;
        } else if (object instanceof com.baidu.hugegraph.structure.HugeVertex) {
            this.serverVertex =
                    (com.baidu.hugegraph.structure.HugeVertex) object;
        } else {
            throw new ComputerException("Invalid vertex type %s",
                                        object.getClass());
        }
    }

    public Vertex convert(InputFilter inputFilter, GraphFactory graphFactory) {
        if (this.clientVertex != null) {
            inputFilter.filter(this);
            Id id = HugeConverter.convertId(this.clientVertex.id());
            String label = this.clientVertex.label();

            Properties properties = HugeConverter.convertProperties(
                    this.clientVertex.properties());
            Vertex computerVertex = graphFactory.createVertex(label, id, null);
            computerVertex.properties(properties);
            return computerVertex;
        } else {
            inputFilter.filter(this);
            Id id = HugeConverter.convertId(this.serverVertex.id().asObject());
            String label = this.serverVertex.label();

            Properties properties = HugeConverter.convertProperties(
                    properties(this.serverVertex));
            Vertex computerVertex = graphFactory.createVertex(label, id, null);
            computerVertex.properties(properties);
            return computerVertex;
        }
    }

    @Override
    public void clearProperties() {
        if (this.clientVertex != null) {
            this.clientVertex.properties().clear();
        } else {
            this.serverVertex.resetProperties();
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
