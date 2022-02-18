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

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.InputSplitFetcher;
import com.baidu.hugegraph.computer.core.input.VertexFetcher;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.MasterInputHandler;
import com.baidu.hugegraph.computer.core.input.GraphFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.computer.core.input.loader.FileVertxFetcher;
import com.baidu.hugegraph.computer.core.input.loader.FileEdgeFetcher;
import com.baidu.hugegraph.computer.core.input.loader.LoaderFileInputSplitFetcher;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LoaderFileGraphFetcherLocal implements GraphFetcher {
    private static final Logger LOG = Log.logger("huge file");

    private MasterInputHandler handler;
    private final VertexFetcher vertexFetcher;
    private final EdgeFetcher edgeFetcher;

    public LoaderFileGraphFetcherLocal(Config config) {
        this.vertexFetcher = new FileVertxFetcher(config);
        this.edgeFetcher = new FileEdgeFetcher(config);

        InputSplitFetcher fetcher = new LoaderFileInputSplitFetcher(config);
        this.handler = new MasterInputHandler(fetcher);
        int vertexSplitSize = this.handler.createVertexInputSplits();
        int edgeSplitSize = this.handler.createEdgeInputSplits();
        LOG.info("Master create {} vertex splits, {} edge splits",
                vertexSplitSize, edgeSplitSize);
    }

    @Override
    public InputSplit nextVertexInputSplit() {
        return this.handler.nextVertexInputSplit();
    }

    @Override
    public InputSplit nextEdgeInputSplit() {
        return this.handler.nextEdgeInputSplit();
    }

    @Override
    public VertexFetcher vertexFetcher() {
        return this.vertexFetcher;
    }

    @Override
    public EdgeFetcher edgeFetcher() {
        return this.edgeFetcher;
    }

    @Override
    public void close() {
        // pass
    }

    public Iterator<Edge> createIteratorFromEdge() {
        return new IteratorFromEdge();
    }

    private class IteratorFromEdge implements Iterator<Edge> {

        private InputSplit currentSplit;

        public IteratorFromEdge() {
            this.currentSplit = null;
        }

        @Override
        public boolean hasNext() {
            EdgeFetcher edgeFetcher = LoaderFileGraphFetcherLocal.
                    this.edgeFetcher;
            try {
                while (this.currentSplit == null || !edgeFetcher.hasNext()) {
                    /*
                     * The first time or the current split is complete,
                     * need to fetch next input split meta
                     */
                    this.currentSplit = LoaderFileGraphFetcherLocal.
                            this.nextEdgeInputSplit();
                    if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                        return false;
                    }
                    edgeFetcher.prepareLoadInputSplit(this.currentSplit);
                }
            } catch (Exception e) {
            }
            return true;
        }

        @Override
        public Edge next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return (Edge)LoaderFileGraphFetcherLocal.this.edgeFetcher.next();
        }
    }

    public Iterator<Vertex> createIteratorFromVertex() {
        return new IteratorFromVertex();
    }

    private class IteratorFromVertex implements Iterator<Vertex> {

        private InputSplit currentSplit;

        public IteratorFromVertex() {
            this.currentSplit = null;
        }

        @Override
        public boolean hasNext() {
            VertexFetcher vertexFetcher =
                    LoaderFileGraphFetcherLocal.this.vertexFetcher;
            try {
                while (this.currentSplit == null || !vertexFetcher.hasNext()) {
                    /*
                     * The first time or the current split is complete,
                     * need to fetch next input split meta
                     */
                    this.currentSplit = LoaderFileGraphFetcherLocal.
                            this.nextVertexInputSplit();
                    if (this.currentSplit.equals(InputSplit.END_SPLIT)) {
                        return false;
                    }
                    vertexFetcher.prepareLoadInputSplit(this.currentSplit);
                }
            } catch (Exception e) {
            }
            return true;
        }

        @Override
        public Vertex next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return (Vertex)LoaderFileGraphFetcherLocal.this.vertexFetcher
                    .next();
        }
    }
}
