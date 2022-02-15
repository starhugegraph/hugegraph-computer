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

package com.baidu.hugegraph.computer.core.compute.input;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.dataparser.DataParser;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.BufferedFileInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.util.Log;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;

public class EdgesInputFast {

    private static final Logger LOG = Log.logger("edges input");

    private RandomAccessInput input;
    private RandomAccessInput vertexinput;
    private ReusablePointer idPointer;
    private ReusablePointer idPointerVertex;
    private ReusablePointer idPointerEdges;
    private final ReusablePointer valuePointer;
    private final ReusablePointer valuePointerVertex;
    private final File edgeFile;
    private final File vertexFile;
    private final GraphFactory graphFactory;
    private final int flushThreshold;
    private final EdgeFrequency frequency;
    private final ComputerContext context;
    private boolean useFixLength;
    private int idBytes;
    private final int edgeLimitNum;
    private static final int UNLIMITED_NUM = -1;
    private final Vertex vertex;

    public EdgesInputFast(ComputerContext context, File edgeFile, 
                                                   File vertexFile) {
        this.graphFactory = context.graphFactory();
        this.idPointer = new ReusablePointer();
        this.idPointerVertex = new ReusablePointer();
        this.idPointerEdges = new ReusablePointer();
        this.valuePointer = new ReusablePointer();
        this.valuePointerVertex = new ReusablePointer();
        this.edgeFile = edgeFile;
        this.flushThreshold = context.config().get(
                              ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
        this.frequency = context.config().get(ComputerOptions.INPUT_EDGE_FREQ);
        this.context = context;
        this.useFixLength = false;
        this.idBytes = 8;
        this.edgeLimitNum = context
            .config().get(ComputerOptions.INPUT_LIMIT_EDGES_IN_ONE_VERTEX);
        this.vertex = context.graphFactory().createVertex();
        this.vertexFile = vertexFile;
    }

    public void init() throws IOException {
        this.input = new BufferedFileInput(this.edgeFile);
        this.vertexinput = new BufferedFileInput(this.vertexFile);
    }

    public void close() throws IOException {
        this.input.close();
        this.vertexinput.close();
    }

    public void switchToFixLength() {
       this.useFixLength = true;
    }

    public void readIdBytes() {
       try {
           this.idBytes = this.input.readFixedInt();
       } catch (IOException e) {
            throw new ComputerException("Can't read from edges input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
       }
    }

    public int readVInt(RandomAccessInput in) throws IOException {
        byte leading = in.readByte();
        int value = leading & 0x7f;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i = 1;
        for (; i < 5; i++) {
            byte b = in.readByte();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
            }
        }
        return value;
    }

    public String readUTF(RandomAccessInput in) throws IOException {
        int length = this.readVInt(in);
        assert length >= 0;
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        return CoderUtil.decode(bytes);
    }
    
    public void reset() {
        try {
            this.input.seek(0);
        } catch (IOException e) {
        }
    }

    public byte[] getOneVertexBuffer() {
        try {
            if (this.vertexinput.available() <= 0) {
                return null;
            }
            long vertexposition = this.vertexinput.position();
            this.idPointerVertex.read(this.vertexinput);
            this.valuePointerVertex.read(this.vertexinput);
            int keylength = (int)this.idPointerVertex.length();
            int valuelength = (int)this.valuePointerVertex.
                                                      length();

            Id id = StreamGraphInput.
                readId(this.idPointerVertex.input());
            
            while (this.input.available() > 0) {             
                long startposition = this.input.position();
                this.idPointerEdges.read(this.input);
                int status = idPointerVertex.
                            compareTo(this.idPointerEdges);

                //System.out.printf("status = %s %d\n", id, status);
                if (status < 0) {
                    //vertex++, return vertex
                    this.input.seek(startposition);
                    this.vertexinput.seek(vertexposition);
                    int length = keylength + valuelength + 8;
                    byte[] datavertex = this.vertexinput.readBytes(length);
                    byte[] data = new byte[length + 8];
                    System.arraycopy(datavertex, 0, data, 0, length);
                    return data;
                } else if (status == 0) {
                    //edge++, return vertex + edge
                    this.vertexinput.seek(vertexposition);
                    int length = keylength + valuelength + 8;
                    byte[] dataVertex = this.vertexinput.readBytes(length);

                    startposition = this.input.position();
                    int valueEdgeLength = this.input.readFixedInt();
                    this.input.seek(startposition);
                    byte[] dataEdges = this.input.
                           readBytes(valueEdgeLength + 4);

                    //only for superedges
                    int count = DataParser.byte2int(dataEdges, 4);
                    int bufferlength = valueEdgeLength + 4;
                    List bufferList = null;
                    while (this.input.available() > 0) {
                        long sp = this.input.position();
                        this.idPointerEdges.read(this.input);
                        if (this.idPointerVertex.
                            compareTo(this.idPointerEdges) != 0) {
                            this.input.seek(sp);
                            break;
                        } 
                        if (bufferList == null) {
                            bufferList = new ArrayList();
                        }
                        int lengthNext = this.input.readFixedInt();
                        byte[] nextEdges = this.input.
                                            readBytes(lengthNext);
                        count += DataParser.byte2int(nextEdges, 0);
                        bufferlength += lengthNext;
                        bufferList.add(nextEdges);
                    }
                    //end superedges

                    //copy vertex and first edge
                    byte[] data = new byte[length + bufferlength];
                    System.arraycopy(dataVertex, 0, data, 0, length);
                    System.arraycopy(dataEdges, 0, data, length, 
                                           valueEdgeLength + 4);

                    //copy more edges into  superedges
                    if (bufferList != null) {
                        int position = length + valueEdgeLength + 4;
                        int ctest = 0;
                        for (Object b : bufferList) {
                            byte[] buffer = (byte[])b;
                            int copylength = buffer.length - 4;
                            System.arraycopy(buffer, 4,
                                   data, position, copylength);
                            position += copylength;
                            ctest++;
                        }
                        byte[] countbyte = DataParser.int2byte(count);
                        System.arraycopy(countbyte, 0,
                                data, length + 4, 4);
                    }
                    return data;
                } else {
                    //skip edge
                    continue;
                }
            }

            //no remaining edge, return vertex
            int length = keylength + valuelength + 8;
            this.vertexinput.seek(vertexposition);
            byte[] datavertex = this.vertexinput.readBytes(length);
            byte[] data = new byte[length + 8];
            System.arraycopy(datavertex, 0, data, 0, length);

            return data;
        } catch (IOException e) {
            return null;
        }
    }

    public Vertex composeVertex(byte[] data, boolean active) {
        if (this.frequency == EdgeFrequency.SINGLE) {
            return composeVertexSingle(data, active);
        }
        else if (this.frequency == EdgeFrequency.SINGLE_PER_LABEL) {
            return composeVertexSinglePerLabel(data, active);
        } else {
            assert this.frequency == EdgeFrequency.MULTIPLE;
            return composeVertexMultiple(data, active);
        }
    }
    
    public Vertex composeVertexSingle(byte[] data, boolean active) {        
        Vertex vertex = context.graphFactory().createVertex();
        
        int position = 4;
        byte code = data[position];
                
        IdType idType = IdType.getIdTypeByCode(code);
        position += 1;
        int idLen = data[position] & 0xFF;
        position += 1;
        byte[] idData = new byte[idLen];
        System.arraycopy(data, position, idData, 0, idLen);    
        Id id = new BytesId(idType, idData, idLen);
        position += idLen;
        vertex.id(id);
        
        position += 4;
        Pair<String, Integer> result = DataParser.
                                         parseUTF(data, position);
        vertex.label(result.getLeft());
        position += result.getRight();

        int[] vint = DataParser.parseVInt(data, position);
        int num = vint[0];
        int shift = vint[1];
        position += shift;

        if (num != 0) {
            Properties propsv = this.graphFactory.createProperties();
            for (int j = 0; j < num; j++) {
                vint = DataParser.parseVInt(data, position);
                int length = vint[0];
                shift = vint[1];
                position += shift;
                String key = CoderUtil.
                                decode(data, position, length);
                position += length;

                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                shift = value.getShift();
                position += shift;

                propsv.put(key, value);
            }
            vertex.properties(propsv);
        }

        position += 4; 
        int count = DataParser.byte2int(data, position);
        Edges edges = this.graphFactory.createEdges(count);
        position += 4;

        if (this.edgeLimitNum != UNLIMITED_NUM &&
                this.edgeLimitNum < count) {
            count = this.edgeLimitNum;
        }

        for (int i = 0; i < count; i++) {
            Edge edge = this.graphFactory.createEdge();
            byte inv = data[position];
            position++;
            
            IdType idT = IdType.getIdTypeByCode(data[position]);
            position++;
            int idL = data[position] & 0xFF;
            position++;
            byte[] idd = new byte[idL];

            System.arraycopy(data, position, idd, 0, idL);   
            Id idTarget = new BytesId(idT, idd, idL);
            edge.targetId(idTarget);
            position += idL;
                        
            idT = IdType.getIdTypeByCode(data[position]);
            position++;
            idL = data[position] & 0xFF;
            position++;
            idd = new byte[idL];
            System.arraycopy(data, position, idd, 0, idL);   
            Id idEdge = new BytesId(idT, idd, idL);
            position += idL;
            edge.id(idEdge);

            vint = DataParser.parseVInt(data, position);
            int length = vint[0];
            shift = vint[1];
            position += shift;
            //byte[] bstring = new byte[length];
            //System.arraycopy(data, position, bstring, 0, length); 
            String label = CoderUtil.decode(data, position, length);
            position += length;
            edge.label(label);
            
            vint = DataParser.parseVInt(data, position);
            num = vint[0];
            shift = vint[1];
            position += shift;

            Properties props = this.graphFactory.createProperties();
            for (int j = 0; j < num; j++) {
                vint = DataParser.parseVInt(data, position);
                length = vint[0];
                shift = vint[1];
                position += shift;
                String key = CoderUtil.
                                decode(data, position, length);
                position += length;

                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                shift = value.getShift();

                //System.out.printf("shift = %d\n", shift);
                position += shift;
                props.put(key, value);
            }
            if (inv == 1) {
                props.put("inv", new BooleanValue(true));
            }  
            edge.properties(props);
            edges.add(edge);
        }
        vertex.edges(edges);
        return vertex;
    }

    public Vertex composeVertexSinglePerLabel(byte[] data, boolean active) {
        Vertex vertex = context.graphFactory().createVertex();
        
        int position = 4;
        byte code = data[position];
                
        IdType idType = IdType.getIdTypeByCode(code);
        position += 1;
        int idLen = data[position] & 0xFF;
        position += 1;
        byte[] idData = new byte[idLen];
        System.arraycopy(data, position, idData, 0, idLen);    
        Id id = new BytesId(idType, idData, idLen);
        position += idLen;
        vertex.id(id);
        
        position += 4;
        Pair<String, Integer> result = DataParser.
                                         parseUTF(data, position);
        vertex.label(result.getLeft());
        position += result.getRight();

        int[] vint = DataParser.parseVInt(data, position);
        int num = vint[0];
        int shift = vint[1];
        position += shift;

        if (num != 0) {
            Properties propsv = this.graphFactory.createProperties();
            for (int j = 0; j < num; j++) {
                vint = DataParser.parseVInt(data, position);
                int length = vint[0];
                shift = vint[1];
                position += shift;
                String key = CoderUtil.
                                decode(data, position, length);
                position += length;

                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                shift = value.getShift();
                position += shift;

                propsv.put(key, value);
            }
            vertex.properties(propsv);
        }

        position += 4; 
        int count = DataParser.byte2int(data, position);
        Edges edges = this.graphFactory.createEdges(count);
        position += 4;

        if (this.edgeLimitNum != UNLIMITED_NUM &&
                this.edgeLimitNum < count) {
            count = this.edgeLimitNum;
        }

        for (int i = 0; i < count; i++) {
            Edge edge = this.graphFactory.createEdge();
            byte inv = data[position];
            position++;
            
            IdType idT = IdType.getIdTypeByCode(data[position]);
            position++;
            int idL = data[position] & 0xFF;
            position++;
            byte[] idd = new byte[idL];

            System.arraycopy(data, position, idd, 0, idL);   
            Id idTarget = new BytesId(idT, idd, idL);
            edge.targetId(idTarget);
            position += idL;
            
            vint = DataParser.parseVInt(data, position);
            int length = vint[0];
            shift = vint[1];
            position += shift;
            //byte[] bstring = new byte[length];
            //System.arraycopy(data, position, bstring, 0, length); 
            String label = CoderUtil.decode(data, position, length);
            position += length;
            edge.label(label);
            
            idT = IdType.getIdTypeByCode(data[position]);
            position++;
            idL = data[position] & 0xFF;
            position++;
            idd = new byte[idL];
            System.arraycopy(data, position, idd, 0, idL);   
            Id idEdge = new BytesId(idT, idd, idL);
            position += idL;
            edge.id(idEdge);

            vint = DataParser.parseVInt(data, position);
            num = vint[0];
            shift = vint[1];
            position += shift;

            Properties props = this.graphFactory.createProperties();
            for (int j = 0; j < num; j++) {
                vint = DataParser.parseVInt(data, position);
                length = vint[0];
                shift = vint[1];
                position += shift;
                String key = CoderUtil.
                                decode(data, position, length);
                position += length;

                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                shift = value.getShift();
                position += shift;

                props.put(key, value);
            }
            if (inv == 1) {
                props.put("inv", new BooleanValue(true));
            }  
            edge.properties(props);
            edges.add(edge);
        }
        vertex.edges(edges);
        return vertex;
    }
   
    public Vertex composeVertexFast(byte[] data, boolean active) {
        if (this.useFixLength) {
            return composeVertexMultipleFastFixedLength(data, active);
        } else {
            return composeVertexMultipleFast(data, active);
        }
    }

    public Vertex composeVertexMultiple(byte[] data, boolean active) {
        Vertex vertex = context.graphFactory().createVertex();
        
        int position = 4;
        byte code = data[position];
                
        IdType idType = IdType.getIdTypeByCode(code);
        position += 1;
        int idLen = data[position] & 0xFF;
        position += 1;
        byte[] idData = new byte[idLen];
        System.arraycopy(data, position, idData, 0, idLen);    
        Id id = new BytesId(idType, idData, idLen);
        position += idLen;
        vertex.id(id);
      
        position += 4;
        Pair<String, Integer> result = DataParser.
                                         parseUTF(data, position);
        vertex.label(result.getLeft());
        position += result.getRight();

        int[] vint = DataParser.parseVInt(data, position);
        int num = vint[0];
        int shift = vint[1];
        position += shift;

        if (num != 0) {
            Properties propsv = this.graphFactory.createProperties();
            for (int j = 0; j < num; j++) {
                vint = DataParser.parseVInt(data, position);
                int length = vint[0];
                shift = vint[1];
                position += shift;
                String key = CoderUtil.
                                decode(data, position, length);
                position += length;

                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                shift = value.getShift();
                position += shift;

                propsv.put(key, value);
            }
            vertex.properties(propsv);
        }

        position += 4; 
        int count = DataParser.byte2int(data, position);
        Edges edges = this.graphFactory.createEdges(count);
        position += 4;

        if (this.edgeLimitNum != UNLIMITED_NUM &&
                this.edgeLimitNum < count) {
            count = this.edgeLimitNum;
        }

        for (int i = 0; i < count; i++) {
            Edge edge = this.graphFactory.createEdge();
            byte inv = data[position];
            position++;
            
            IdType idT = IdType.getIdTypeByCode(data[position]);
            position++;
            int idL = data[position] & 0xFF;
            position++;
            byte[] idd = new byte[idL];

            System.arraycopy(data, position, idd, 0, idL);   
            Id idTarget = new BytesId(idT, idd, idL);
            edge.targetId(idTarget);
            position += idL;
            
            vint = DataParser.parseVInt(data, position);
            int length = vint[0];
            shift = vint[1];
            position += shift;
            //byte[] bstring = new byte[length];
            //System.arraycopy(data, position, bstring, 0, length); 
            String label = CoderUtil.decode(data, position, length);
            position += length;
            edge.label(label);
            
            vint = DataParser.parseVInt(data, position);
            length = vint[0];
            shift = vint[1];
            position += shift;
            String name = CoderUtil.decode(data, position, length);
            position += length;
            edge.name(name);

            idT = IdType.getIdTypeByCode(data[position]);
            position++;
            idL = data[position] & 0xFF;
            position++;
            idd = new byte[idL];
            System.arraycopy(data, position, idd, 0, idL);   
            Id idEdge = new BytesId(idT, idd, idL);
            position += idL;
            edge.id(idEdge);

            vint = DataParser.parseVInt(data, position);
            num = vint[0];
            shift = vint[1];
            position += shift;

            Properties props = this.graphFactory.createProperties();
            for (int j = 0; j < num; j++) {
                vint = DataParser.parseVInt(data, position);
                length = vint[0];
                shift = vint[1];
                position += shift;
                String key = CoderUtil.
                                decode(data, position, length);
                position += length;

                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                shift = value.getShift();
                position += shift;

                props.put(key, value);
            }
            if (inv == 1) {
                props.put("inv", new BooleanValue(true));
            }  
            edge.properties(props);
            edges.add(edge);
        }
        vertex.edges(edges);
        return vertex;
    }
    
    public Vertex composeVertexMultipleFastFixedLength
                               (byte[] data, boolean active) {
        Vertex vertex = context.graphFactory().createVertex();
        
        int position = 4;
        byte[] blId = new byte[8];
        for (int j = 0; j < this.idBytes; j++) {
            int j_ = j + Long.BYTES - this.idBytes;
            blId[j_] = data[position + j];
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(blId, 0, Long.BYTES);
        buffer.flip();
        Long lId = buffer.getLong();
        Id id = BytesId.of(lId);
        position += 8;
        vertex.id(id);
      
        //label
        position += 4;
        int[] vint = DataParser.parseVInt(data, position);
        position += vint[0];
        position += vint[1];

        //property 
        vint = DataParser.parseVInt(data, position);
        position += vint[1];

        for (int j = 0; j < vint[0]; j++) {
            int[] vintj = DataParser.parseVInt(data, position);
            position += vintj[0];
            position += vintj[1];
            ValueType valueType = ValueType.
                        getValueTypeByCode(data[position]);
            position += 1;     
            Value<?> value = this.graphFactory.
                                    createValue(valueType);       
            value.parse(data, position);
            position +=  value.getShift();
        }

        //
        position += 4; 
        int count = DataParser.byte2int(data, position);
        Edges edges = this.graphFactory.createEdges(count);
        position += 4;

        if (this.edgeLimitNum != UNLIMITED_NUM &&
                this.edgeLimitNum < count) {
            count = this.edgeLimitNum;
        }

        for (int i = 0; i < count; i++) {
            position++;
            
            byte[] blIde = new byte[8];
            for (int j = 0; j < this.idBytes; j++) {
                int j_ = j + Long.BYTES - this.idBytes;
                blIde[j_] = data[position + j];
            }
            ByteBuffer buffere = ByteBuffer.allocate(Long.BYTES);
            buffere.put(blIde, 0, Long.BYTES);
            buffere.flip();
            Long lIde = buffere.getLong();
            Id idTarget = BytesId.of(lIde);
            position += 8;
            Edge edge = new DefaultEdge(idTarget);

            vint = DataParser.parseVInt(data, position);
            position += vint[0];
            position += vint[1];
            
            vint = DataParser.parseVInt(data, position);
            position += vint[0];
            position += vint[1];

            vint = DataParser.parseVInt(data, position);
            position += vint[1];

            for (int j = 0; j < vint[0]; j++) {
                int[] vintj = DataParser.parseVInt(data, position);
                position += vintj[0];
                position += vintj[1];
                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                position +=  value.getShift();
            }
            edges.add(edge);
        }
        vertex.edges(edges);
        return vertex;
    }


    public Vertex composeVertexMultipleFast(byte[] data, boolean active) {
        Vertex vertex = context.graphFactory().createVertex();
        
        int position = 4;
        byte code = data[position];
        IdType idType = IdType.getIdTypeByCode(code);
        position += 1;
        int idLen = data[position] & 0xFF;
        position += 1;
        byte[] idData = new byte[idLen];
        System.arraycopy(data, position, idData, 0, idLen);    
        Id id = new BytesId(idType, idData, idLen);
        position += idLen;
        vertex.id(id);
      
        //label
        position += 4;
        int[] vint = DataParser.parseVInt(data, position);
        position += vint[0];
        position += vint[1];

        //property 
        vint = DataParser.parseVInt(data, position);
        position += vint[1];

        for (int j = 0; j < vint[0]; j++) {
            int[] vintj = DataParser.parseVInt(data, position);
            position += vintj[0];
            position += vintj[1];
            ValueType valueType = ValueType.
                        getValueTypeByCode(data[position]);
            position += 1;     
            Value<?> value = this.graphFactory.
                                    createValue(valueType);       
            value.parse(data, position);
            position +=  value.getShift();
        }

        //
        position += 4; 
        int count = DataParser.byte2int(data, position);
        Edges edges = this.graphFactory.createEdges(count);
        position += 4;

        if (this.edgeLimitNum != UNLIMITED_NUM &&
                this.edgeLimitNum < count) {
            count = this.edgeLimitNum;
        }

        for (int i = 0; i < count; i++) {
            position++;
            
            IdType idT = IdType.getIdTypeByCode(data[position]);
            position++;
            int idL = data[position] & 0xFF;

            position++;
            byte[] idd = new byte[idL];
            System.arraycopy(data, position, idd, 0, idL);   
            Id idTarget = new BytesId(idT, idd, idL);
            Edge edge = new DefaultEdge(idTarget);
            position += idL;
            
            vint = DataParser.parseVInt(data, position);
            position += vint[0];
            position += vint[1];
            
            vint = DataParser.parseVInt(data, position);
            position += vint[0];
            position += vint[1];

            position++;
            idL = data[position] & 0xFF;

            position += 1 + idL;

            vint = DataParser.parseVInt(data, position);
            position += vint[1];

            for (int j = 0; j < vint[0]; j++) {
                int[] vintj = DataParser.parseVInt(data, position);
                position += vintj[0];
                position += vintj[1];
                ValueType valueType = ValueType.
                            getValueTypeByCode(data[position]);
                position += 1;     
                Value<?> value = this.graphFactory.
                                        createValue(valueType);       
                value.parse(data, position);
                position +=  value.getShift();
            }
            edges.add(edge);
        }
        vertex.edges(edges);
        return vertex;
    }

    public void readAllEdges() {
        RandomAccessInput in = this.input;
        try {
            long startPosition = in.position();
            while (this.input.available() > 0) {
                //id pointer 
                ReusablePointer idPointer = new ReusablePointer();
                idPointer.read(in);
                Id id = new BytesId();
                id.read(idPointer.input());

                //value pointer
                int l2 = in.readFixedInt();
                int count = in.readFixedInt();
                for (int i = 0; i < count; i++) {
                    byte binv = in.readByte();
                    StreamGraphInput.readId(in);
                    this.readUTF(in);
                    this.readUTF(in);
                    //StreamGraphInput.readLabel(in);
                    StreamGraphInput.readId(in);
                    int propsint = this.readVInt(in);
                    //Properties props = this.graphFactory.createProperties();
                    //props.read(in);
                }
            }
            in.seek(startPosition);
        }
        catch (IOException e) {
            throw new ComputerException("Can't read from edges input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
        }
    }

    public Edges edges(ReusablePointer vidPointer) {
        try {
            while (this.input.available() > 0) {
                long startPosition = this.input.position();
                int status = -1;
                this.idPointer.read(this.input);
                status = vidPointer.compareTo(this.idPointer);
                if (status < 0) {
                    /*
                     * No edges, the current batch belong to vertex that
                     * vertex id is bigger than specified id.
                     */
                    this.input.seek(startPosition);
                    return EmptyEdges.instance();
                } else if (status == 0) {
                    // Has edges
                    this.valuePointer.read(this.input);
                    Edges edges = this.readEdges(this.valuePointer.input());
                    return edges;
                    //if need super edges
                    //return new SuperEdges(vidPointer, edges, startPosition);
                } else {
                    /*
                     * The current batch belong to vertex that vertex id is
                     * smaller than specified id.
                     */
                    int valueLength = this.input.readFixedInt();
                    this.input.skip(valueLength);
                }
            }
            return EmptyEdges.instance();
        } catch (IOException e) {
            throw new ComputerException("Can't read from edges input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
        }
    }
    
    /**
     * Read edges & attach it by input stream, also could limit the edges here
     * TODO: use one reused Edges instance to read batches for each vertex &
     *       limit edges in early step (like input/send stage)
     */
    private Edges readEdges(RandomAccessInput in) {
        try {
            long pos0 = in.position();
            // Could limit edges to read here (unlimited by default)
            int count = in.readFixedInt();
          
            // update count when "-1 < limitNum < count"
            if (this.edgeLimitNum != UNLIMITED_NUM &&
                this.edgeLimitNum < count) {
                count = this.edgeLimitNum;
            }

            Edges edges = this.graphFactory.createEdges(count);
            if (this.frequency == EdgeFrequency.SINGLE) {
                for (int i = 0; i < count; i++) {
                    Edge edge = this.graphFactory.createEdge();
                    // Only use targetId as subKey, use props as subValue
                    long p0 = in.position();
                    boolean inv = (in.readByte() == 1) ? true : false;
                    if (!this.useFixLength) {
                        edge.targetId(StreamGraphInput.readId(in));
                    } 
                    else {
                       byte[] bId = in.readBytes(this.idBytes);
                       byte[] blId = new byte[8];
                       for (int j = 0; j < this.idBytes; j++) {
                           int j_ = j + Long.BYTES - this.idBytes;
                           blId[j_] = bId[j];
                       }
                       ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                       buffer.put(blId, 0, Long.BYTES);
                       buffer.flip();
                       Long lId = buffer.getLong();
                       edge.targetId(this.context.
                                      graphFactory().createId(lId)); 
                    }
                    // Read subValue
                    if (!this.useFixLength) {
                        edge.id(StreamGraphInput.readId(in));
                    }
                    edge.label(StreamGraphInput.readLabel(in));
                    
                    Properties props = this.graphFactory.createProperties();
                    props.read(in);
                    edge.properties(props);
                    if (inv) {
                        Properties properties = edge.properties();
                        properties.put("inv", new BooleanValue(true));
                        edge.properties(properties);
                    }         
                    edges.add(edge);
                }
            } else if (this.frequency == EdgeFrequency.SINGLE_PER_LABEL) {
                for (int i = 0; i < count; i++) {
                    Edge edge = this.graphFactory.createEdge();
                    // Use label + targetId as subKey, use props as subValue
                    boolean inv = (in.readByte() == 1) ? true : false;
                    //edge.label(StreamGraphInput.readLabel(in));
                    if (!this.useFixLength) {
                        edge.targetId(StreamGraphInput.readId(in));
                    }
                    else {
                       byte[] bId = in.readBytes(this.idBytes);
                       byte[] blId = new byte[8];
                       for (int j = 0; j < this.idBytes; j++) {
                           int j_ = j + Long.BYTES - this.idBytes;
                           blId[j_] = bId[j];
                       }
                       ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                       buffer.put(blId, 0, Long.BYTES);
                       buffer.flip();
                       Long lId = buffer.getLong();
                       edge.targetId(this.context.
                                      graphFactory().createId(lId));
                    }
                    edge.label(StreamGraphInput.readLabel(in));
                    // Read subValue
                    if (!this.useFixLength) {
                        edge.id(StreamGraphInput.readId(in));
                    }
                    Properties props = this.graphFactory.createProperties();
                    props.read(in);
                    edge.properties(props);
                    if (inv) {
                        Properties properties = edge.properties();
                        properties.put("inv", new BooleanValue(true));
                        edge.properties(properties);
                    }
                    edges.add(edge);
                }
            } else {
                assert this.frequency == EdgeFrequency.MULTIPLE;
                for (int i = 0; i < count; i++) {
                    Edge edge = this.graphFactory.createEdge();
                    /*
                     * Use label + sortValues + targetId as subKey,
                     * use properties as subValue
                     */
                    boolean inv = (in.readByte() == 1) ? true : false;
                    //edge.label(StreamGraphInput.readLabel(in));
                    //edge.name(StreamGraphInput.readLabel(in));
                    if (!this.useFixLength) {
                        edge.targetId(StreamGraphInput.readId(in));
                    }
                    else {
                       byte[] bId = in.readBytes(this.idBytes);
                       byte[] blId = new byte[8];
                       for (int j = 0; j < this.idBytes; j++) {
                           int j_ = j + Long.BYTES - this.idBytes;
                           blId[j_] = bId[j];
                       }
                       ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                       buffer.put(blId, 0, Long.BYTES);
                       buffer.flip();
                       Long lId = buffer.getLong();
                       edge.targetId(this.context.
                                      graphFactory().createId(lId));
                    }

                    edge.label(StreamGraphInput.readLabel(in));
                    edge.name(StreamGraphInput.readLabel(in));

                    // Read subValue
                    if (!this.useFixLength) {
                        edge.id(StreamGraphInput.readId(in));
                    }
                    // Read properties
                    Properties props = this.graphFactory.createProperties();
                    props.read(in);
                    edge.properties(props);
                    if (inv) {
                        Properties properties = edge.properties();
                        properties.put("inv", new BooleanValue(true));
                        edge.properties(properties);
                    }
                    edges.add(edge);
                }
            }
            return edges;
        } catch (IOException e) {
            throw new ComputerException("Failed to read edges from input '%s'",
                                        e, this.edgeFile.getAbsoluteFile());
        }
    }

    public static class EmptyEdges implements Edges {

        private static final EmptyEdges INSTANCE = new EmptyEdges();

        private EmptyEdges() {
            // pass
        }

        public static EmptyEdges instance() {
            return INSTANCE;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void add(Edge edge) {
            throw new ComputerException(
                      "Not support adding edges during computing");
        }

        @Override
        public Iterator<Edge> iterator() {
            return Collections.emptyIterator();
        }
    }
}
