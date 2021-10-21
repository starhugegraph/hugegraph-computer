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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.aggregator.MasterAggrManager;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.rpc.AggregateRpcService;
import com.baidu.hugegraph.util.Log;

/**
 * Used for master to control input & output (same like ComputeManager),
 *
 * TODO: Better to unify later (rename ComputerManager to WorkerComputerManager
 * and extends new abstract class ComputerManager)
 */
public class MasterComputeManager {

    private static final Logger LOG = Log.logger(MasterComputeManager.class);

    // Shall we need to support get data from partitions? (or only by workers)
    private final MasterAggrManager aggrManager;

    public MasterComputeManager(Managers managers) {
        // Shall we use MasterComputerContext? (need refactor)
        this.aggrManager = managers.get(MasterAggrManager.NAME);
    }

    public void output() {
        LOG.info("##### Master output start here #####");
        // We need get aggregate value from worker first, then combine them
        AggregateRpcService handler = this.aggrManager.handler();
        // Output if need
        handler.listAggregators().entrySet().forEach(aggr -> {
            LOG.info("Current aggregator name is {}, value is {}",
                     aggr.getKey(), aggr.getValue());
        });
        LOG.info("##### Master output end here #####");
    }
}
