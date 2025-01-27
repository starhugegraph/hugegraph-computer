/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.hugegraph.computer.dist;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.master.MasterService;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.util.ComputerContextUtil;
import com.baidu.hugegraph.computer.core.worker.WorkerService;
import com.baidu.hugegraph.computer.core.worker.WorkerServiceLouvain;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.Properties;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;



public class HugeGraphComputer {

    private static final Logger LOG = Log.logger(HugeGraphComputer.class);

    private static final String ROLE_MASTER = "master";
    private static final String ROLE_WORKER = "worker";
    private static final String ROLE_WORKERS = "workers";
    /**
     *  Some class must be load first, in order to invoke static method to init;
     */
    private static void loadClass() throws ClassNotFoundException {
        Class.forName(IdType.class.getCanonicalName());
        Class.forName(MessageType.class.getCanonicalName());
        Class.forName(ValueType.class.getCanonicalName());
    }

    public static void main(String[] args) throws IOException,
                                                  ClassNotFoundException {
        E.checkArgument(ArrayUtils.getLength(args) == 4,
                        "Argument count must be three, " +
                        "the first is conf path;" +
                        "the second is role type;" +
                        "the third is drive type;" +
                        "the forth is do which step");
      
        String role = args[1];
        E.checkArgument(!StringUtils.isEmpty(role),
                        "The role can't be null or emtpy, " +
                        "it must be either '%s' or '%s'",
                        ROLE_MASTER, ROLE_WORKER, ROLE_WORKERS);
        String useMode = args[3];

        setUncaughtExceptionHandler();
        loadClass();
        registerOptions();
        ComputerContext context = parseContext(args[0]);

        String algorithm = getAlgorithmParam(args[0]);
        System.out.println("algorithm:" + algorithm);
        switch (role) {
            case ROLE_MASTER:
                if (!algorithm.contains("LouvainParams"))
                    executeMasterService(context); //todo
                break;
            case ROLE_WORKER:
                if (algorithm.contains("LouvainParams"))
                    executeWorkerServiceLouvain(context);
                else
                    executeWorkerService(context, useMode);  //todo
                break;
            default:
                throw new IllegalArgumentException(
                          String.format("Unexpected role '%s'", role));
        }
    }

    protected static void setUncaughtExceptionHandler() {
        Thread.UncaughtExceptionHandler handler =
               Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(
               new PrintExceptionHandler(handler)
        );
    }

    private static class PrintExceptionHandler implements
                                               Thread.UncaughtExceptionHandler {

        private final Thread.UncaughtExceptionHandler handler;

        PrintExceptionHandler(Thread.UncaughtExceptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            HugeGraphComputer.LOG.error("Failed to run service on {}, {}",
                                        t, e.getMessage(), e);
            if (this.handler != null) {
                this.handler.uncaughtException(t, e);
            }
        }
    }

    private static void executeWorkerService(ComputerContext context, 
                                                 String useMode) {
        try (WorkerService workerService = new WorkerService()) {
            workerService.setUseMode(useMode);
            workerService.init(context.config());
            workerService.execute();
        }
    }

    private static void executeWorkerServiceLouvain(ComputerContext context) {
        try (WorkerServiceLouvain workerService = new WorkerServiceLouvain()) {
            workerService.init(context.config());
            workerService.execute();
        }
    }

    private static void executeMasterService(ComputerContext context) {
        try (MasterService masterService = new MasterService()) {
            masterService.init(context.config());
            masterService.execute();
        }
    }

    private static ComputerContext parseContext(String conf)
                                                throws IOException {
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(
                                            new FileReader(conf));
        properties.load(bufferedReader);
        ComputerContextUtil.initContext(properties);
        
        return ComputerContext.instance();
    }

    private static String getAlgorithmParam(String conf)
            throws IOException {
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader(conf));
        properties.load(bufferedReader);

        return properties.getProperty(
                ComputerOptions.ALGORITHM_PARAMS_CLASS.name());
    }

    private static void registerOptions() {
        OptionSpace.register("computer",
                             "com.baidu.hugegraph.computer.core.config." +
                             "ComputerOptions");
        OptionSpace.register("computer-rpc",
                             "com.baidu.hugegraph.config.RpcOptions");
    }
}
