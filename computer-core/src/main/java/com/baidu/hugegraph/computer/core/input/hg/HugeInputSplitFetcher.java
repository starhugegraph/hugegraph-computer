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

package com.baidu.hugegraph.computer.core.input.hg;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.util.Log;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.computer.core.input.InputSplitFetcher;

public class HugeInputSplitFetcher implements InputSplitFetcher {

    private static final Logger LOG = Log.logger("huge fetcher");
    private static final String GRAPH_SUFFIX = "/g";

    private final Config config;

    private PDClient pdClient;
    private List<Metapb.Partition> partitions;

    public HugeInputSplitFetcher(Config config) {
        this.config = config;
        String pdPeers = this.config.get(ComputerOptions.INPUT_PD_PEERS);
        this.pdClient = PDClient.create(
                        PDConfig.of(pdPeers).setEnablePDNotify(true));
        this.partitions = new ArrayList<>();
    }

    @Override
    public void close() {
        // pass
    }

    @Override
    public List<InputSplit> fetchVertexInputSplits() {
        return this.fetchInputSplits();
    }

    @Override
    public List<InputSplit> fetchEdgeInputSplits() {
        return this.fetchInputSplits();
    }

    private List<InputSplit> fetchInputSplits() {
        // Format is {graphspace}/{graph}
        String graph = this.config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        graph = graph + GRAPH_SUFFIX;
        try {
            if (this.partitions == null || this.partitions.isEmpty()) {
                this.partitions = this.pdClient.getPartitions(0, graph);
            }
        } catch (PDException e) {
            e.printStackTrace();
        }

        List<InputSplit> splits = new ArrayList<>();
        for (Metapb.Partition partition : this.partitions) {
            InputSplit split = new InputSplit(
                               String.valueOf(partition.getStartKey()),
                               String.valueOf(partition.getEndKey()));
            splits.add(split);
        }
        return splits;
    }
}
