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
import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


public class MessageInputFast<T extends Value<?>> {

    private final Config config;
    private final PeekableIterator<KvEntry> messages;
    private T value;
    Pointer idPointer = null;

    public MessageInputFast(ComputerContext context,
                        PeekableIterator<KvEntry> messages,
                        boolean inCompute) {
        if (messages == null) {
            this.messages = PeekableIterator.emptyIterator();
        } else {
            this.messages = messages;
        }
        this.config = context.config();

        if (!inCompute) {
            this.value = (T)(new IdList());
        }
        else {
            this.value = this.config.createObject(
                     ComputerOptions.ALGORITHM_MESSAGE_CLASS);
        }
    }

    public Iterator<T> iterator(long vid) {
        while (this.messages.hasNext()) {
            KvEntry entry = this.messages.peek();
            Pointer key = entry.key();
            try {
                byte[] bkey = key.bytes();
                int length = bkey[1];
                byte[] blId = new byte[length];
                for (int j = 0; j < length; j++) {
                    blId[j] = bkey[j + 2];
                }
                BytesId id = new BytesId(IdType.LONG, blId);
                long lid = (long)id.asObject();
                if (lid > vid) {
                    return Collections.emptyIterator();
                }
                else if (lid == vid) {
                    ReusablePointer vidPointer = 
                                    new ReusablePointer(bkey,
                                                     bkey.length);
                    return new MessageIterator(vidPointer);
                }
                else {
                    this.messages.next();
                }
            } catch (IOException e) {
                throw new ComputerException("read id error", e);
            }
        }
        return Collections.emptyIterator();
    }
    
    private Id parseId(byte[] keydata) {
        int position = 0;
        byte bvalue = keydata[position];
        SerialEnum.register(IdType.class);
        IdType idType = SerialEnum.fromCode(IdType.class, bvalue);
        position += 1;
        int idLen = keydata[position];
        position += 1;
        byte[] idData = new byte[idLen];
        System.arraycopy(keydata, position, idData, 0, idLen);    
        Id id = new BytesId(idType, idData, idLen);
        return id;
    }
    
    public Pair<Id, List> readOneVertexMessage() {
        int count = 0;
        List<T> values = new ArrayList<>();
        try {
            while (this.messages.hasNext()) {
                KvEntry entry = this.messages.peek();
                Pointer key = entry.key();
                Pointer value = entry.value();

                Id id0 = parseId(key.bytes());
                //System.out.printf("message %s \n", id0);
                
                if (this.idPointer == null) {
                    this.idPointer = key;
                    continue;
                }
                
                int status = this.idPointer.compareTo(key);
                if (status < 0) {
                    Id id = parseId(idPointer.bytes());
    
                    Pair<Id, List> pair = new ImmutablePair<>(id, values);
                    //System.out.printf("!! %s %f\n", id,
                    // values.get(0).value());
                    this.idPointer = key;
                    //System.out.printf("%s %d\n", id, values.size());
                    
                    return pair;
                }
                byte[] valuedata = value.bytes();

                BytesInput in = IOFactory.createBytesInput(valuedata);
                T valuei = this.config.createObject(
                     ComputerOptions.ALGORITHM_MESSAGE_CLASS);
                valuei.read(in);
                
                values.add(valuei);
                this.messages.next();

                if (!this.messages.hasNext()) {
                    Id id = parseId(idPointer.bytes());
                    Pair<Id, List> pair = new ImmutablePair<>(id, values);
                    this.idPointer = key;
                    
                    //System.out.printf("%s %d\n", id, values.size());
                    //System.out.printf("!! %s %f\n", id, 
                    //values.get(0).value());

                    return pair;
                }
            }
        } catch (IOException e) {
                throw new ComputerException("Can't read value", e);
        }
        
        Pair<Id, List> p_ = new ImmutablePair<>(null, null);
        return p_;
    }
    
    public Iterator<T> iterator(ReusablePointer vidPointer) {
        while (this.messages.hasNext()) {
            KvEntry entry = this.messages.peek();
            Pointer key = entry.key();
            Pointer value = entry.value();
            int status = vidPointer.compareTo(key);
            if (status < 0) {
                return Collections.emptyIterator();
            } else if (status == 0) {
                break;
            } else {
                this.messages.next();
            }
        }

        return new MessageIterator(vidPointer);
    }

    public void close() throws Exception {
        this.messages.close();
    }

    private class MessageIterator implements Iterator<T> {

        // It indicates whether the value can be returned to client.
        private boolean valueValid;
        private ReusablePointer vidPointer;

        private MessageIterator(ReusablePointer vidPointer) {
            this.vidPointer = vidPointer;
            this.valueValid = false;
        }

        @Override
        public boolean hasNext() {
            if (this.valueValid) {
                return true;
            }
            if (MessageInputFast.this.messages.hasNext()) {
                KvEntry entry = MessageInputFast.this.messages.peek();
                Pointer key = entry.key();
                int status = this.vidPointer.compareTo(key);
                if (status == 0) {
                    MessageInputFast.this.messages.next();
                    this.valueValid = true;
                    try {
                        BytesInput in = IOFactory.createBytesInput(
                                        entry.value().bytes());
                        MessageInputFast.this.value.read(in);
                    } catch (IOException e) {
                        throw new ComputerException("Can't read value", e);
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        @Override
        public T next() {
            if (this.valueValid) {
                this.valueValid = false;
                return MessageInputFast.this.value;
            } else {
                throw new NoSuchElementException();
            }
        }
    }
}