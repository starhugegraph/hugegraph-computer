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

package com.baidu.hugegraph.computer.algorithm.community.louvain.input;

//import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.VertexFetcher;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.GraphFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class GraphFetcherLocal implements GraphFetcher {
    private static final Logger LOG = Log.logger("GraphFetcherLocal");

    /*public GraphFetcherLocal(Config config, MasterInputHandler handler) {
        this.vertexFetcher = new FileVertxFetcher(config);
        this.edgeFetcher = new FileEdgeFetcher(config);

        this.handler = handler;
    }*/

    public Iterator<Object> createIteratorFromEdge() {
        return new IteratorFromEdge();
    }

    private class IteratorFromEdge implements Iterator<Object> {

        private InputSplit currentSplit;

        public IteratorFromEdge() {
            this.currentSplit = null;
        }

        @Override
        public boolean hasNext() {
            EdgeFetcher edgeFetcher = edgeFetcher();
            try {
                while (this.currentSplit == null || !edgeFetcher.hasNext()) {
                    /*
                     * The first time or the current split is complete,
                     * need to fetch next input split meta
                     */
                    this.currentSplit = nextEdgeInputSplit();
                    if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                        return false;
                    }
                    edgeFetcher.prepareLoadInputSplit(this.currentSplit);
                }
            } catch (Exception e) {
                LOG.error("IteratorFromEdge:", e);
                //return false;
            }
            return true;
        }

        @Override
        public Object next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return edgeFetcher().next();
        }
    }



    public Iterator<Object> createIteratorFromVertex() {
        return new IteratorFromVertex();
    }

    private class IteratorFromVertex implements Iterator<Object> {

        private InputSplit currentSplit;

        public IteratorFromVertex() {
            this.currentSplit = null;
        }

        @Override
        public boolean hasNext() {
            VertexFetcher vertexFetcher = vertexFetcher();
            try {
                while (this.currentSplit == null || !vertexFetcher.hasNext()) {
                    /*
                     * The first time or the current split is complete,
                     * need to fetch next input split meta
                     */
                    this.currentSplit = nextVertexInputSplit();
                    if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                        return false;
                    }
                    vertexFetcher.prepareLoadInputSplit(this.currentSplit);
                }
            } catch (Exception e) {
                LOG.error("IteratorFromVertex:", e);
                //return false;
            }
            return true;
        }

        @Override
        public Object next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return vertexFetcher().next();
        }
    }
}
