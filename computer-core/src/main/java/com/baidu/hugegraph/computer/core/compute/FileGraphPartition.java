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

package com.baidu.hugegraph.computer.core.compute;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.compute.input.EdgesInputFast;
import com.baidu.hugegraph.computer.core.compute.input.MessageInputFast;
import com.baidu.hugegraph.computer.core.compute.input.VertexInput;
import com.baidu.hugegraph.computer.core.compute.output.EdgesOutput;
import com.baidu.hugegraph.computer.core.compute.output.VertexOutput;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.BufferedFileInput;
import com.baidu.hugegraph.computer.core.io.BufferedFileOutput;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.output.ComputerOutput;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.FileGenerator;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class FileGraphPartition {

    private static final Logger LOG = Log.logger("partition");

    private static final String VERTEX = "vertex";
    private static final String EDGE = "edge";
    private static final String STATUS = "status";
    private static final String VALUE = "value";

    private final ComputerContext context;
    private final Computation<Value<?>> computation;
    private final FileGenerator fileGenerator;
    private final int partition;

    private final File vertexFile;
    private final File edgeFile;
    private final File vertexComputeFile;
    private final File edgeComputeFile;

    private File preStatusFile;
    private File curStatusFile;
    private File preValueFile;
    private File curValueFile;

    private long vertexCount;
    private long edgeCount;

    private BufferedFileOutput curStatusOutput;
    private BufferedFileOutput curValueOutput;

    private BufferedFileInput preStatusInput;
    private BufferedFileInput preValueInput;

    private VertexInput vertexInput;
    private EdgesInputFast edgesInput;
    private VertexInput vertexOriginInput;

    private MessageInputFast<Value<?>> messageInput;

    private final MessageSendManager sendManager;
    private boolean useVariableLengthOnly;
    private String useMode;

    public FileGraphPartition(ComputerContext context,
                              Managers managers,
                              int partition, String mode) {
        this.context = context;
        this.computation = context.config()
                                  .createObject(
                                   ComputerOptions.WORKER_COMPUTATION_CLASS);
        this.computation.init(context.config());
        this.fileGenerator = managers.get(FileManager.NAME);
        this.partition = partition;
        if (mode == "all") {
            this.vertexFile = new File(this.fileGenerator.
                                       randomDirectory(VERTEX));
            this.edgeFile = new File(this.fileGenerator.
                                       randomDirectory(EDGE));
        }
        else {

            String vertexFileName  = this.fileGenerator.
                             fixDirectory(partition, "VERTEX");
            String edgeFileName  = this.fileGenerator.
                             fixDirectory(partition, "EDGE");     
            this.vertexFile = new File(vertexFileName);
            this.edgeFile = new File(edgeFileName);
        }
      
        this.useMode = mode;
        this.vertexComputeFile = 
                           new File(this.fileGenerator.randomDirectory(VERTEX));
        this.edgeComputeFile = 
                           new File(this.fileGenerator.randomDirectory(EDGE));
        this.vertexCount = 0L;
        this.edgeCount = 0L;
        this.sendManager = managers.get(MessageSendManager.NAME);
        this.useVariableLengthOnly = false;
    }

    protected PartitionStat input(PeekableIterator<KvEntry> vertices,
                                  PeekableIterator<KvEntry> edges) {
        try {
            createFile(this.vertexFile);
            createFile(this.edgeFile);
            createFile(this.vertexComputeFile);
            createFile(this.edgeComputeFile);
            BufferedFileOutput vertexOut = new BufferedFileOutput(
                                           this.vertexFile);
            BufferedFileOutput edgeOut = new BufferedFileOutput(
                                         this.edgeFile);
            while (vertices.hasNext()) {
                KvEntry entry = vertices.next();
                Pointer key = entry.key();
                Pointer value = entry.value();
                this.writeVertex(key, value, vertexOut);
                this.writeEdges(key, edges, edgeOut);
            }
            vertexOut.close();
            edgeOut.close();
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to init FileGraphPartition '%s'",
                      e, this.partition);
        }
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount, 0L);
    }

    protected PartitionStat compute0(WorkerContext context) {
        this.computation.beforeSuperstep(context);

        long activeVertexCount = 0L;
        try {
            this.beforeCompute(0);
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep 0", e);
        }

        BlockingQueue<Vertex> vertexQueue =
                                new ArrayBlockingQueue<Vertex>(100);
        BlockingQueue<Vertex> inputQueue = 
                                new ArrayBlockingQueue<Vertex>(100);
        
        ComputeConsumer computeConsumer = 
                        new ComputeConsumer(context, this.computation, 
                                curValueOutput, curStatusOutput,
                                vertexQueue, null,
                                this.vertexCount);
                                                            
        UnSerializeConsumerProducer unSerializeConsumerProducer =
                        new  UnSerializeConsumerProducer (this.context,
                                inputQueue, vertexQueue, 
                                this.edgesInput);
                                    
        Thread t1 = new Thread(computeConsumer);
        t1.start();
                    
        Thread t2 = new Thread(unSerializeConsumerProducer);
        t2.start();
      
        while (true) {
            try {
                byte[] data = this.edgesInput.getOneVertexBuffer();
                if (data == null) {
                    break;
                }

                Vertex rawVertex = this.context.graphFactory().
                                                createVertex();
                rawVertex.data(data);
                inputQueue.put(rawVertex);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        activeVertexCount = computeConsumer.getActiveVertexCount();

        unSerializeConsumerProducer.stop();
        computeConsumer.stop();
 
        try {
            this.afterCompute(0);
        } catch (Exception e) {
            throw new ComputerException("Error occurred when afterCompute", e);
        }

        this.computation.afterSuperstep(context);

        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount,
                                 this.vertexCount - activeVertexCount);
    }

    public void sendIdHash(ComputationContext context) {
        try {
            //this.beforeCompute(-1);
            this.vertexInput = new VertexInput(this.context,
                                       this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInputFast(this.context, 
                                                 this.edgeFile,
                                                 this.vertexFile);
            this.vertexInput.init();
            this.edgesInput.init();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, -1);
        }
        int selfIncreaseID = 0;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            Edges edges = this.edgesInput.edges(this.vertexInput.idPointer());
            for (Edge edge : edges) {
                BooleanValue inv = edge.properties().get("inv");
                boolean inv_ = (inv == null) ? false : inv.value();
                if (!inv_) {
                    continue;
                }

                Id targetId = edge.targetId();
                long nid = (((long) this.partition) << 32) |
                           (selfIncreaseID & 0xffffffffL);
                Id id = this.context.graphFactory().createId(nid);
                IdList path = new IdList();
                path.add(vertex.id());
                path.add(id);

                this.sendManager.sendHashIdMessage(targetId, path);
            }
            selfIncreaseID++;
        }
        try {
            this.vertexInput.close();
            this.edgesInput.close();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, -1);
        }
    }

    public void partitionHashId() {
        try {
            //this.beforeCompute(-1);
            this.vertexInput = new VertexInput(this.context,
                                       this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInputFast(this.context,
                                                 this.edgeFile,
                                                 this.vertexFile);
            this.vertexInput.init();
            this.edgesInput.init();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, -1);
        }

        VertexOutput vertexOutput = new VertexOutput(
                                    this.context, this.vertexComputeFile);
        EdgesOutput edgesOutput = new EdgesOutput(
                                  this.context, this.edgeComputeFile);
        try {
            vertexOutput.init();
            edgesOutput.init();

            vertexOutput.switchToFixLength();
            edgesOutput.switchToFixLength();

            vertexOutput.writeIdBytes();
            edgesOutput.writeIdBytes();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %d",
                      e, 0);
        }

        LOG.info("{} begin hash and write id", this);
        long selfIncreaseID = 0;
        while (this.vertexInput.hasNext()) {
            Vertex vertex = this.vertexInput.next();
            Id id = this.context.graphFactory().createId(selfIncreaseID);
            vertex.id(id);
            vertexOutput.writeVertex(vertex);

            Iterator<Value<?>> messageIter = this.messageInput.iterator(
                                      this.vertexInput.idPointer());
 
            Edges edges = this.edgesInput.edges(
                              this.vertexInput.idPointer());
            Iterator<Edge> it = edges.iterator();
            edgesOutput.startWriteEdge(vertex);
            while (messageIter.hasNext()) {
                if (it.hasNext()) {
                    Edge edge = it.next();

                    IdList idList = (IdList)(messageIter.next());
                    Id originId = idList.get(0);
                    Id newId = idList.get(1);
             
                    edge.targetId(newId);
                    edgesOutput.writeEdge(edge);
                }
                else {
                }
            }
            edgesOutput.finishWriteEdge();
            selfIncreaseID++;
        }
        LOG.info("{} end hash and write id", this);

        try {
            this.vertexInput.close();
            this.edgesInput.close();
            vertexOutput.close();
            edgesOutput.close();
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %d",
                      e, 0);
        }
    }

    public void setVertexCount(long vertexCount) {
        this.vertexCount = vertexCount;
    }

    public void setEdgeCount(long edgeCount) {
        this.edgeCount = edgeCount;
    }

    public long getVertexCount() {
        return this.vertexCount;
    }

    public long getEdgeCount() {
        return this.edgeCount;
    }

    protected PartitionStat compute(WorkerContext context, int superstep) {
        this.computation.beforeSuperstep(context);

        try {
            this.beforeCompute(superstep);
        } catch (IOException e) {
            throw new ComputerException(
                      "Error occurred when beforeCompute at superstep %s",
                      e, superstep);
        }

        long activeVertexCount = 0L;
        BlockingQueue<Vertex> vertexQueue =
            new ArrayBlockingQueue<Vertex>(100);
        BlockingQueue<Vertex> inputQueue = 
            new ArrayBlockingQueue<Vertex>(100);
        BlockingQueue<Pair<Id, List>> messageQueue = 
                        new ArrayBlockingQueue<Pair<Id, List>>(100);
        
        UnSerializeConsumerProducer unSerializeConsumerProducer =
                new  UnSerializeConsumerProducer(this.context,
                                    inputQueue, vertexQueue,
                                    this.edgesInput);

        ComputeConsumer computeConsumer = 
                        new ComputeConsumer(context, this.computation, 
                                curValueOutput, curStatusOutput,
                                vertexQueue, messageQueue,
                                this.vertexCount);
                                
        MessageProducer messageProducer = 
                        new MessageProducer(this.messageInput, 
                                            messageQueue,
                                            computeConsumer);

        Thread t0 = new Thread(computeConsumer);
        t0.start();
    
        Thread t1 = new Thread(unSerializeConsumerProducer);
        t1.start();

        Thread t2 = new Thread(messageProducer);
        t2.start();
    
        while (true) {
            try {
                byte[] data = this.edgesInput.getOneVertexBuffer();
                if (data == null) {
                    break;
                }
    
                Vertex rawVertex = this.context.graphFactory().
                                                  createVertex();
                rawVertex.data(data);
                Value<?> result = this.context.config().createObject(
                    ComputerOptions.ALGORITHM_RESULT_CLASS);
                this.readVertexStatusAndValue(rawVertex, result);
    
                inputQueue.put(rawVertex);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        computeConsumer.normoreVertex();
        activeVertexCount = computeConsumer.getActiveVertexCount();
        
        try {
            this.afterCompute(superstep);
        } catch (Exception e) {
            throw new ComputerException(
                      "Error occurred when afterCompute at superstep %s",
                      e, superstep);
        }

        messageProducer.stop();
        unSerializeConsumerProducer.stop();
        computeConsumer.stop();
      

        this.computation.afterSuperstep(context);
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount,
                                 this.vertexCount - activeVertexCount);
    }

    protected PartitionStat output() {
        ComputerOutput output = this.context.config().createObject(
                                ComputerOptions.OUTPUT_CLASS);
        output.init(this.context.config(), this.partition);
        try {
            this.beforeOutput();
        } catch (IOException e) {
            throw new ComputerException("Error occurred when beforeOutput", e);
        }

        Value<?> result = this.context.config().createObject(
                          ComputerOptions.ALGORITHM_RESULT_CLASS);
        while (true) {
            byte[] data = this.edgesInput.getOneVertexBuffer();
            if (data == null) {
                break;
            }
            Vertex vertex = this.edgesInput.
                    composeVertex(data, true);
            this.readVertexStatusAndValue(vertex, result);

            if (!this.useVariableLengthOnly) {
                if (this.vertexOriginInput.hasNext()) {
                    Vertex vertex1 = this.vertexOriginInput.next();
                    vertex.id(vertex1.id());
                }
            }
            output.write(vertex);
        }

        try {
            this.afterOutput();
        } catch (IOException e) {
            throw new ComputerException("Error occurred when afterOutput", e);
        }
        output.close();
        return new PartitionStat(this.partition, this.vertexCount,
                                 this.edgeCount, 0L);
    }

    /**
     * Put the messages sent at previous superstep from MessageRecvManager to
     * this partition. The messages is null if no messages sent to this
     * partition at previous superstep.
     */
    protected void messages(PeekableIterator<KvEntry> messages, 
                                              boolean inCompute) {
        if (!inCompute) {
            this.messageInput = new MessageInputFast<>(this.context,
                                                   messages, false);
        }
        else {
            this.messageInput = new MessageInputFast<>(this.context, 
                                                   messages, true);
        }
    }

    protected int partition() {
        return this.partition;
    }

    private void readVertexStatusAndValue(Vertex vertex, Value<?> result) {
        try {
            boolean activate = this.preStatusInput.readBoolean();
            if (activate) {
                vertex.reactivate();
            } else {
                vertex.inactivate();
            }
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to read status of vertex %s", e, vertex);
        }

        try {
            result.read(this.preValueInput);
            vertex.value(result);
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to read value of vertex %s", e, vertex);
        }
    }

    private void writeVertex(Pointer key, Pointer value,
                             BufferedFileOutput vertexOut) throws IOException {
        byte[] keyBytes = key.bytes();
        vertexOut.writeFixedInt(keyBytes.length);
        vertexOut.write(keyBytes);

        byte[] valueBytes = value.bytes();
        vertexOut.writeFixedInt(valueBytes.length);
        vertexOut.write(valueBytes);

        this.vertexCount++;
    }

    private void writeEdges(Pointer vid, PeekableIterator<KvEntry> edges,
                            BufferedFileOutput edgeOut) throws IOException {
        byte[] vidBytes = vid.bytes();
        while (edges.hasNext()) {
            KvEntry entry = edges.peek();
            Pointer key = entry.key();
            int matched = vid.compareTo(key);
            if (matched < 0) {
                return;
            }

            edges.next();
            if (matched > 0) {
                // Skip stale edges
                continue;
            }
            edgeOut.writeFixedInt(vidBytes.length);
            edgeOut.write(vidBytes);

            long valuePosition = edgeOut.position();
            edgeOut.writeFixedInt(0);

            this.edgeCount += entry.numSubEntries();
            edgeOut.writeFixedInt((int) entry.numSubEntries());
            EntryIterator subKvIt = EntriesUtil.subKvIterFromEntry(entry);
            while (subKvIt.hasNext()) {
                KvEntry subEntry = subKvIt.next();
                // Not write sub-key length
                edgeOut.write(subEntry.key().bytes());
                // Not write sub-value length
                edgeOut.write(subEntry.value().bytes());
            }
            long valueLength = edgeOut.position() - valuePosition -
                               Constants.INT_LEN;
            edgeOut.writeFixedInt(valuePosition, (int) valueLength);
        }
    }

    //only for test when no id map applied
    public void useVariableLengthOnly() {
        this.useVariableLengthOnly = true;
    }

    private void beforeCompute(int superstep) throws IOException {

        if (this.useVariableLengthOnly) {
            //LOG.info("{} workerservice use variable length id", this);
            this.vertexInput = new VertexInput(this.context, 
                                        this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInputFast(this.context, 
                                                 this.edgeFile,
                                                 this.vertexFile);
            
            this.vertexInput.init();
            this.edgesInput.init();
        }
        else {
            LOG.info("{} workerservice use fix length id", this);
            this.vertexInput = new VertexInput(this.context, 
                                    this.vertexComputeFile, this.vertexCount);
            this.edgesInput = new EdgesInputFast(this.context, 
                                    this.edgeComputeFile,
                                    this.vertexFile);
            this.vertexInput.init();
            this.edgesInput.init();
            this.vertexInput.switchToFixLength();
            this.edgesInput.switchToFixLength();
            this.vertexInput.readIdBytes();
            this.edgesInput.readIdBytes();
        }

        if (superstep > 0) {
            this.preStatusFile = this.curStatusFile;
            this.preValueFile = this.curValueFile;
            this.preStatusInput = new BufferedFileInput(this.preStatusFile);
            this.preValueInput = new BufferedFileInput(this.preValueFile);
        }

        // Outputs of vertex's status and vertex's value.
        String statusPath = this.fileGenerator.randomDirectory(
                            STATUS, Integer.toString(superstep),
                            Integer.toString(this.partition));
        String valuePath = this.fileGenerator.randomDirectory(
                           VALUE, Integer.toString(superstep),
                           Integer.toString(this.partition));
        this.curStatusFile = new File(statusPath);
        this.curValueFile = new File(valuePath);
        createFile(this.curStatusFile);
        createFile(this.curValueFile);

        this.curStatusOutput = new BufferedFileOutput(this.curStatusFile);
        this.curValueOutput = new BufferedFileOutput(this.curValueFile);
    }

    private void afterCompute(int superstep) throws Exception {
        this.vertexInput.close();
        this.edgesInput.close();
        if (superstep > 0) {
            this.messageInput.close();
            this.preStatusInput.close();
            this.preValueInput.close();
            this.preStatusFile.delete();
            this.preValueFile.delete();
        }
        this.curStatusOutput.close();
        this.curValueOutput.close();
    }

    private void beforeOutput() throws IOException {
        if (this.useVariableLengthOnly) {
            this.vertexInput = new VertexInput(this.context, 
                                     this.vertexFile, this.vertexCount);
            this.edgesInput = new EdgesInputFast(this.context, 
                                                this.edgeFile,
                                                this.vertexFile);

            this.vertexInput.init();
            this.edgesInput.init();
        }
        else {
            this.vertexOriginInput = new VertexInput(this.context,
                                     this.vertexFile, this.vertexCount);
            this.vertexInput = new VertexInput(this.context, 
                                     this.vertexComputeFile, this.vertexCount);
            this.edgesInput = new EdgesInputFast(this.context, 
                                     this.edgeComputeFile,
                                     this.vertexComputeFile);
            this.vertexOriginInput.init();
            this.vertexInput.init();
            this.edgesInput.init();

            vertexInput.switchToFixLength();
            edgesInput.switchToFixLength();

            vertexInput.readIdBytes();
            edgesInput.readIdBytes();
        }

        this.preStatusFile = this.curStatusFile;
        this.preValueFile = this.curValueFile;
        this.preStatusInput = new BufferedFileInput(this.preStatusFile);
        this.preValueInput = new BufferedFileInput(this.preValueFile);
    }

    private void afterOutput() throws IOException {
        this.vertexInput.close();
        this.edgesInput.close();

        this.preStatusInput.close();
        this.preValueInput.close();

        assert this.preStatusFile == this.curStatusFile;
        assert this.preValueFile == this.curValueFile;
        this.preStatusFile.delete();
        this.preValueFile.delete();

        if (this.useMode.equals("all")) {
            this.vertexFile.delete();
            this.edgeFile.delete();
        }

        this.vertexComputeFile.delete();
        this.edgeComputeFile.delete();
    }

    private static void createFile(File file) throws IOException {
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        E.checkArgument(file.createNewFile(), "Already exists file: %s", file);
    }
 
    private class UnSerializeConsumerProducer implements Runnable {
   
        protected BlockingQueue vertexQueue = null;
        protected BlockingQueue inputQueue = null;
        protected EdgesInputFast edgesInput = null;
        protected ComputerContext context = null;
        protected boolean stop = false;
        protected boolean stoped = false;
 
        public UnSerializeConsumerProducer(
                        ComputerContext context,
                        BlockingQueue inputQueue,
                        BlockingQueue vertexQueue, 
                        EdgesInputFast edgesInput) {
            this.context = context;
            this.inputQueue = inputQueue;
            this.vertexQueue = vertexQueue;
            this.edgesInput = edgesInput;
        }

        public void stop() {
            this.stop = true;
            while (!this.stoped) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
        }

        //parse vertex id, edge target id only
        public void runShortSchema() {
            while (!this.stop) {
                try {
                    Vertex rawVertex = (Vertex)this.inputQueue
                                    .poll(50, TimeUnit.MILLISECONDS);  
                    if (rawVertex == null) {
                        continue;
                    }

                    Value<?> value = rawVertex.value();                    
                    boolean active = rawVertex.active();
                    Vertex vertex = this.edgesInput.
                        composeVertexFast(rawVertex.data(), active);
                    
                    if (value != null) {
                        vertex.value(rawVertex.value());
                    }
                    if (active) {
                        vertex.reactivate();
                    } else {
                        vertex.inactivate();
                    }
                    this.vertexQueue.put(vertex); 
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
            this.stoped = true;
        }

        //parse all data structure
        public void runFullSchema() {
            while (!this.stop) {
                try {
                    Vertex rawVertex = (Vertex)this.inputQueue
                                    .poll(50, TimeUnit.MILLISECONDS);  
                    if (rawVertex == null) {
                        continue;
                    }

                    Value<?> value = rawVertex.value();                    
                    boolean active = rawVertex.active();
                    Vertex vertex = this.edgesInput.
                        composeVertex(rawVertex.data(), active);
        
                    if (value != null) {
                        vertex.value(rawVertex.value());
                    }
                    if (active) {
                        vertex.reactivate();
                    } else {
                        vertex.inactivate();
                    }
                    this.vertexQueue.put(vertex); 
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
            this.stoped = true;
        }

        @Override
        public void run() {
            String fastComposer = this.context.config().
                get(ComputerOptions.USE_FASTER_COMPOSER);
            if (fastComposer.equals("full")) {
                this.runFullSchema();
            } else {
                assert fastComposer.equals("targetidonlly");
                this.runShortSchema();
            }
     
        }
    }

    private class MessageProducer implements Runnable {
        protected MessageInputFast messageInput;
        protected BlockingQueue messageQueue;
        protected ComputeConsumer computeConsumer;
        protected boolean stop = false;
        protected boolean stoped = false;

        public MessageProducer(MessageInputFast messageInput,
                               BlockingQueue messageQueue,
                               ComputeConsumer computeConsumer) {
            this.messageInput = messageInput;
            this.messageQueue = messageQueue;
            this.computeConsumer = computeConsumer;
        }
        
        @Override
        public void run() {
            try {
                while (!this.stop) {
                    Pair<Id, List> message = 
                        this.messageInput.readOneVertexMessage();
                    if (message.getKey() == null) {
                        this.computeConsumer.nomoreMessage();
                        break;
                    }
                    this.messageQueue.put(message);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } 
            this.stoped = true;
        }

        public void stop() {
            this.stop = true;
            while (!this.stoped) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
        }
    }

    private class ComputeConsumer implements Runnable {
        protected ComputationContext context;
        protected Computation<Value<?>> computation;
        protected MessageInputFast messageInput;
        protected BlockingQueue vertexQueue = null;
        protected BlockingQueue messageQueue = null;
        protected BufferedFileOutput curValueOutput;
        protected BufferedFileOutput curStatusOutput;
        protected long activeVertexCount;
        long vertexcount;
        long consumecount = 0;
        boolean expectMessage = true;
        boolean expectVertex = true;
        protected boolean stop = false;
        protected boolean stoped = false;
    
        public ComputeConsumer(ComputationContext context,
                               Computation<Value<?>> computation, 
                               BufferedFileOutput curValueOutput, 
                               BufferedFileOutput curStatusOutput, 
                               BlockingQueue vertexQueue,
                               BlockingQueue messageQueue,
                               long vertexcount) {
            this.computation = computation;
            this.context = context;
            this.curValueOutput = curValueOutput;
            this.curStatusOutput = curStatusOutput;
            this.vertexQueue = vertexQueue;
            this.messageQueue = messageQueue;
            this.vertexcount = vertexcount;
            activeVertexCount = 0;
        }
        
        @Override
        public void run() {
            Pair<Id, List> message = null;
            while (!this.stop) {           
                try {
                    Vertex vertex = (Vertex)this.vertexQueue.
                                  poll(50, TimeUnit.MILLISECONDS);  
                    if (vertex == null) {
                        //in case there are wild message 
                        //belongs to nobody
                        //consume all messages
                        if (!this.expectVertex) {
                            while (this.messageQueue.size() != 0) {
                                this.messageQueue.take();
                            }
                        }
                        continue;
                    }
                    Id idv = vertex.id();
                    if (this.messageQueue == null) {
                        //compute 0, no message
                        this.computation.compute0(context, vertex);
                    }
                    else {
                        // compute, possible with message
                        int status = -1;
                        Iterator messageIter = Collections.emptyIterator();
                        
                        if (message == null) {
                            while (this.expectMessage) {
                                message = (Pair<Id, List>)this.messageQueue.
                                        poll(10, TimeUnit.MILLISECONDS);  
                                if (message != null) {
                                    break;
                                } 
                            }
                        }

                        if (message != null) {
                            Id messageId = message.getKey();
                            List messagelist = message.getValue();
                            messageIter = messagelist.iterator();
                            status = idv.compareTo(messageId);
                        }

                        if (status < 0) {
                            //has no message
                            //either message belongs to next vertex
                            //or no more expect message
                            if (vertex.active()) {
                                this.computation.compute(context, vertex, 
                                        Collections.emptyIterator());
                            }
                            //keep the message
                        }
                        else if (status == 0) {
                            //has message and reactivate vertex
                            vertex.reactivate();
                            this.computation.compute(context, vertex, 
                                    messageIter);
                            //consume the message
                            message = null;
                        }
                        else {
                            //something wrong 
                            messageQueue.poll(100, TimeUnit.MILLISECONDS); 
                            message = null;
                        }
                    }
                    if (vertex.active()) {
                        activeVertexCount++;
                    }
                    this.saveVertex(vertex);
                    this.consumecount++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            this.stoped = true;
        }

        public void stop() {
            this.stop = true;
            while (!this.stoped) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
        }

        public void normoreVertex() {
            this.expectVertex = false;
        }
       
        public void nomoreMessage() {
            while (this.messageQueue.size() != 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
            this.expectMessage = false;
        }

        public long getActiveVertexCount() {
            while (this.consumecount < this.vertexcount) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
            this.consumecount = 0;
            return this.activeVertexCount;
        }
        
        private void saveVertex(Vertex vertex) throws IOException {
            this.curStatusOutput.writeBoolean(vertex.active());
            Value<?> value = vertex.value();
            value.write(this.curValueOutput);
        }
    }
}
