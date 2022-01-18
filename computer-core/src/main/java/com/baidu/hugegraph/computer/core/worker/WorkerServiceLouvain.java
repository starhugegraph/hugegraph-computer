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

package com.baidu.hugegraph.computer.core.worker;

import java.io.Closeable;

import com.baidu.hugegraph.computer.core.graph.value.Value;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.util.ShutdownHook;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class WorkerServiceLouvain implements Closeable {

    private static final Logger LOG = Log.logger(WorkerServiceLouvain.class);

    private final ComputerContext context;

    private volatile boolean inited;
    private volatile boolean closed;
    private Config config;

    private volatile ShutdownHook shutdownHook;
    private volatile Thread serviceThread;

    private WorkerStat inputWorkerStat;
    private boolean useIdFixLength;
    private Computation<Value<?>> computation;

    public WorkerServiceLouvain() {
        this.context = ComputerContext.instance();
        this.inited = false;
        this.closed = false;
        this.shutdownHook = new ShutdownHook();
    }

    /**
     * Init worker service, create the managers used by worker service.
     */
    public void init(Config config) {
        E.checkArgument(!this.inited, "The %s has been initialized", this);

        this.serviceThread = Thread.currentThread();
        this.registerShutdownHook();

        this.config = config;

        this.inited = true;
    }

    private void registerShutdownHook() {
        this.shutdownHook.hook(() -> {
            this.stopServiceThread();
        });
    }

    /**
     * Stop the worker service. Stop the managers created in
     * {@link #init(Config)}.
     */
    @Override
    public void close() {
        this.checkInited();
        if (this.closed) {
            LOG.info("{} WorkerService had closed before", this);
            return;
        }

        this.shutdownHook.unhook();

        this.closed = true;
        LOG.info("{} WorkerService closed", this);
    }

    private void stopServiceThread() {
        if (this.serviceThread == null) {
            return;
        }

        try {
            this.serviceThread.interrupt();
        } catch (Throwable ignore) {
        }
    }


    /**
     * Execute the superstep in worker. It first wait master witch superstep
     * to start from. And then do the superstep iteration until master's
     * superstepStat is inactive.
     */
    public void execute() {
        this.checkInited();

        LOG.info("{} WorkerService execute", this);

        this.computation = this.config.createObject(
                ComputerOptions.WORKER_COMPUTATION_CLASS);
        LOG.info("Loading computation '{}' in category '{}'",
                this.computation.name(), this.computation.category());

        this.computation.init(this.config);
    }

    @Override
    public String toString() {
        Object id = "?" + this.hashCode();
        return String.format("[worker %s]", id);
    }

    private void checkInited() {
        E.checkArgument(this.inited, "The %s has not been initialized", this);
    }


}
