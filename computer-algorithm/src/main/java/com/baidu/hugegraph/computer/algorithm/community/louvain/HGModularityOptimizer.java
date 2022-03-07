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

/**
 * Support load data and output result from hugegraph
 */

package com.baidu.hugegraph.computer.algorithm.community.louvain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
//import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.NotSupportedException;

import com.baidu.hugegraph.computer.algorithm.community.louvain.input.GraphFetcherLocal;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.value.StringValue;
import com.baidu.hugegraph.computer.core.input.HugeConverter;
import com.baidu.hugegraph.computer.core.input.InputSplitFetcher;
import com.baidu.hugegraph.computer.core.input.MasterInputHandler;
import com.baidu.hugegraph.computer.core.input.hg.HugeInputSplitFetcher;
import com.baidu.hugegraph.computer.core.io.BufferedFileInput;
import com.baidu.hugegraph.computer.core.io.BufferedFileOutput;
import com.baidu.hugegraph.computer.core.output.ComputerOutput;
import com.baidu.hugegraph.util.ExecutorUtil;
//import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.algorithm.community.louvain.input.HugeGraphFetcherLocal;
import com.baidu.hugegraph.computer.algorithm.community.louvain.input.LoaderFileGraphFetcherLocal;
import com.baidu.hugegraph.computer.core.config.Config;
//import com.baidu.hugegraph.structure.graph.Edge;
//import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.TimeUtil;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class HGModularityOptimizer {

    private static final Logger LOG = Log.logger(HGModularityOptimizer.class);
    private static final int ALGORITHM = 1;
    private final int initialCapacity;

    private final Config config;
    private BiMap<Object, Integer> idMap;
    private final String weightKey;
    private final String delimiter;
    private int maxId;
    private final ComputerContext context;
    private final Vertex vertex;
    private final String idFileName;

    public static final String OPTION_CAPACITY = "louvain.capacity";
    public static final String OPTION_WEIGHTKEY = "louvain.weightkey";
    public static final String OPTION_DELIMITER = "louvain.delimiter";
    public static final String OPTION_MODULARITY = "louvain.modularity";
    public static final String OPTION_RESOLUTION = "louvain.resolution";
    public static final String OPTION_RANDOMSTART = "louvain.randomstart";
    public static final String OPTION_ITERATIONS = "louvain.iterations";
    public static final String OPTION_RANDOMSEED = "louvain.randomseed";

    public HGModularityOptimizer(Config config) {
        this.context = ComputerContext.instance();
        this.vertex = context.graphFactory().createVertex();
        this.config = config;
        this.maxId = -1;
        this.initialCapacity = config.getInt(OPTION_CAPACITY,50000000);
        System.out.println("initialCapacity:" + this.initialCapacity);
        this.idMap = HashBiMap.create(this.initialCapacity);
        this.weightKey = config.getString(OPTION_WEIGHTKEY,"");
        this.delimiter = config.getString(OPTION_DELIMITER," ");
        this.idFileName = "idfile.dat";
    }

    public void runAlgorithm() throws IOException {
        int algorithm = ALGORITHM;
        int modularityFunction = config.getInt(OPTION_MODULARITY,1);
        double resolution = config.getDouble(OPTION_RESOLUTION,1.0);
        int nRandomStarts = config.getInt(OPTION_RANDOMSTART,1);
        int nIterations = config.getInt(OPTION_ITERATIONS,1);
        long randomSeed = config.getLong(OPTION_RANDOMSEED,100);

        VOSClusteringTechnique vOSClusteringTechnique;
        double modularity, maxModularity, resolution2;
        int i, j;

        LOG.info("Modularity Optimizer version 1.3.0 by Ludo Waltman and " +
                 "Nees Jan van Eck");

        LOG.info("Start input data...");
        StopWatch watcher = new StopWatch();
        watcher.start();

        String inputType = config.get(ComputerOptions.INPUT_SOURCE_TYPE);

        Network network;
        switch (inputType) {
            case "hugegraph":
                network = this.readFromHG(modularityFunction, 0);
                break;
            case "loader":
                //network = this.readFromHdfs(modularityFunction);
                network = this.readFromHG(modularityFunction, 1);
                break;
            default:
                throw new NotSupportedException(
                        "not support inputType: " + inputType);
        }

        watcher.stop();
        LOG.info("Number of nodes: {}", network.getNNodes());
        LOG.info("Number of edges: {}", network.getNEdges());
        E.checkArgument(network.getNNodes() > 0, "nNodes must be > 0");
        LOG.info("End input data, cost: {}",
                 TimeUtil.readableTime(watcher.getTime()));

        watcher.reset();
        watcher.start();
        if (algorithm == 1) {
            LOG.info("Running Louvain algorithm...");
        } else if (algorithm == 2) {
            LOG.info("Running Louvain algorithm with multilevel refinement...");
        } else if (algorithm == 3) {
            LOG.info("Running smart local moving algorithm...");
        }

        resolution2 = ((modularityFunction == 1) ?
                       (resolution / (2 * network.getTotalEdgeWeight() +
                                      network.totalEdgeWeightSelfLinks)) :
                       resolution);

        Clustering clustering = null;
        maxModularity = Double.NEGATIVE_INFINITY;
        Random random = new Random(randomSeed);
        for (i = 0; i < nRandomStarts; i++) {
            if (nRandomStarts > 1) {
                LOG.info("Random start: {}", i + 1);
            }
            vOSClusteringTechnique = new VOSClusteringTechnique(network,
                                                                resolution2);

            j = 0;
            boolean update = true;
            do {
                if (nIterations > 1) {
                    LOG.info("Iteration: {}", j + 1);
                }
                if (algorithm == 1) {
                    update = vOSClusteringTechnique.runLouvainAlgorithm(random);
                } else if (algorithm == 2) {
                    update = vOSClusteringTechnique
                            .runLouvainAlgorithmWithMultilevelRefinement(
                                    random);
                } else if (algorithm == 3) {
                    vOSClusteringTechnique.runSmartLocalMovingAlgorithm(random);
                }
                j++;
                modularity = vOSClusteringTechnique.calcQualityFunction();
                if (nIterations > 1) {
                    LOG.info("Modularity: {}", modularity);
                }
            } while ((j < nIterations) && update);

            if (modularity > maxModularity) {
                clustering = vOSClusteringTechnique.getClustering();
                maxModularity = modularity;
            }

            if (nRandomStarts > 1) {
                if (nIterations == 1) {
                    LOG.info("Modularity: {}", modularity);
                }
            }
        }

        if (nRandomStarts == 1) {
            LOG.info("Modularity: {}", maxModularity);
        } else {
            LOG.info("Maximum modularity in {} random starts: {}",
                     nRandomStarts, maxModularity);
        }

        watcher.stop();
        LOG.info("Elapsed time: {}", TimeUtil.readableTime(watcher.getTime()));

        LOG.info("Start output...");
        watcher.reset();
        watcher.start();

        ComputerOutput output = this.config.createObject(
                ComputerOptions.OUTPUT_CLASS);
        output.init(this.config, 1);
        this.writeOutput(clustering,output);
        output.close();

        /*
        String outputType = config.getString(OPTION_OUTPUTTYPE,"hugegraph");
        switch (outputType) {
            case "hugegraph":
                this.writeOutputHg(clustering);
                break;
            case "file":
                String outputFilePath = config.getString(OPTION_OUTPUTPATH,"");
                this.writeOutputFile(outputFilePath, clustering);
                break;
            default:
                throw new NotSupportedException(
                        "not support outputType: " + outputType);
        }*/
        watcher.stop();
        LOG.info("End output, cost:{}",
                 TimeUtil.readableTime(watcher.getTime()));
    }

    public class Entry {
        public int srcid;
        public int targetid;
        public float weight;

        public Entry(int srcid, int targetid, float weight) {
            this.srcid = srcid;
            this.targetid = targetid;
            this.weight = weight;
        }
    }

    private Network readFromHG(int modularityFunction, int inputType) {
        int i, j, nEdges;
        //Entry[] edgeList = new Entry[1];
        //List<Entry> edgeList = new ArrayList<>(1470000000);
        //List<Integer> node1 = new ArrayList<>(1470000000);
        //(this.initialCapacity);
        //List<Integer> node2 = new LinkedList<>();
        //List<Double> edgeWeight1 = new LinkedList<>();
        AtomicInteger edgenums = new AtomicInteger(0);
        long lastTime = 0;

        StopWatch watcher = new StopWatch();
        watcher.start();
        Integer parallelNum = this.config.get(
                ComputerOptions.INPUT_PARALLEL_NUM);
        try {
            InputSplitFetcher fetcher = new HugeInputSplitFetcher(this.config);
            MasterInputHandler handler = new MasterInputHandler(fetcher);
            int vertexSplitSize = handler.createVertexInputSplits();
            int edgeSplitSize = handler.createEdgeInputSplits();
            LOG.info("Master create {} vertex splits, {} edge splits",
                    vertexSplitSize, edgeSplitSize);

            ExecutorService loadExecutor = ExecutorUtil.newFixedThreadPool(
                    parallelNum, "load-data-%d");
            List<GraphFetcherLocal> hgFetchers = new ArrayList<>();
            for (int t = 0; t < parallelNum; t++) {
                if (inputType == 0)
                    hgFetchers.add(
                        new HugeGraphFetcherLocal(this.config, handler));
                else
                    hgFetchers.add(new
                            LoaderFileGraphFetcherLocal(this.config, handler));
            }

            List<Future<?>> futures = new ArrayList<>(parallelNum);
            for (int t = 0; t < parallelNum; t++) {
                final int[] tArray = new int[]{t};
                GraphFetcherLocal hgFetcher = hgFetchers.get(t);
                Future<?> future = loadExecutor.submit(() -> {
                    Iterator<Object> iterator =
                            hgFetcher.createIteratorFromEdge();
                    try {
                        File file = new File("edgelist.dat" + tArray[0]);
                        if (file.exists()) {
                            file.delete();
                        }
                        BufferedFileOutput bufferedWriterEdge =
                                new BufferedFileOutput(file);
                        long lasthgTime = 0;
                        while (iterator.hasNext()) {
                            Integer sourceId = 0;
                            Integer targetId = 0;
                            Float weight = 1.0f;

                            Object edgeobj = iterator.next();
                            if (System.currentTimeMillis() - lasthgTime >=
                                    TimeUnit.SECONDS.toMillis(30L)) {
                                LOG.info("Loading edge: {}, nums:{}", edgeobj,
                                        edgenums.get() + 1);
                                lasthgTime = System.currentTimeMillis();
                            }

                            /*if (inputType == 0) {
                                com.baidu.hugegraph.structure.HugeEdge edge =
                                        (com.baidu.hugegraph.structure.HugeEdge)
                                                edgeobj;
                                sourceId = this.covertId(HugeConverter.
                                        convertId(edge.sourceVertex().id().
                                                asObject()).asObject());

                                targetId = this.covertId(HugeConverter.
                                        convertId(edge.targetVertex().id().
                                                asObject()).asObject());

                                if (StringUtils.isNotBlank(this.weightKey)) {
                                    Float weight_ = (Float)
                                        edge.property(this.weightKey).value();
                                    if (weight_ != null) {
                                        weight = weight_;
                                    }
                                }
                            }
                            else {
                                com.baidu.hugegraph.structure.graph.Edge edge =
                                    (com.baidu.hugegraph.structure.graph.Edge)
                                            edgeobj;
                                sourceId = this.covertId(edge.sourceId());
                                targetId = this.covertId(edge.targetId());

                                if (StringUtils.isNotBlank(this.weightKey)) {
                                    Float weight_ = (Float)
                                            edge.property(this.weightKey);
                                    if (weight_ != null) {
                                        weight = weight_;
                                    }
                                }
                            }

                            bufferedWriterEdge.writeInt(sourceId);
                            bufferedWriterEdge.writeInt(targetId);
                            bufferedWriterEdge.writeFloat(weight); */

                            edgenums.incrementAndGet();
                        }
                        bufferedWriterEdge.close();
                    } catch (Exception e) {
                        LOG.error("readFromHG:", e);
                    }
                });
                futures.add(future);
            }
            for (Future<?> future : futures) {
                future.get();
            }


            LOG.info("start load vertex from hugegraph");
            futures.clear();
            for (GraphFetcherLocal hgFetcher : hgFetchers) {
                Future<?> future = loadExecutor.submit(() -> {
                    Iterator<Object>
                            iteratorV = hgFetcher.createIteratorFromVertex();
                    while (iteratorV.hasNext()) {
                        if (inputType == 0) {
                            com.baidu.hugegraph.structure.HugeVertex vertex =
                                    (com.baidu.hugegraph.structure.HugeVertex)
                                            iteratorV.next();
                            this.covertId(HugeConverter.convertId(
                                    vertex.id().asObject()).asObject());
                        }
                        else {
                            com.baidu.hugegraph.structure.graph.Vertex vertex =
                                    (com.baidu.hugegraph.structure.graph.Vertex)
                                            iteratorV.next();
                                this.covertId(vertex.id());
                        }
                    }
                });
                futures.add(future);
            }
            for (Future<?> future : futures) {
                future.get();
            }
            loadExecutor.shutdown();

            for (GraphFetcherLocal hgFetcher : hgFetchers) {
                hgFetcher.close();
            }

            futures.clear();
            handler = null;

            writeIdFile(this.idFileName, this.maxId + 1);
            this.idMap = null;
            System.gc();

        } catch (Exception e) {
            LOG.error("readFromHG:", e);
        }
        watcher.stop();

        int nums = edgenums.get();
        LOG.info("Load data complete, cost: {}, nums: {}",
                TimeUtil.readableTime(watcher.getTime()),
                nums);

        int nNodes = this.maxId + 1;
        int[] nNeighbors = new int[nNodes];
        try {
            LOG.info("start load edge id and weight from file");

            int count = 0;
            for (int t = 0; t < parallelNum; t++) {
                BufferedFileInput bufferedReaderEdge = new BufferedFileInput(
                        new File("edgelist.dat" + t));
                while (bufferedReaderEdge.available() > 0) {
                    if (System.currentTimeMillis() - lastTime >=
                            TimeUnit.SECONDS.toMillis(30L)) {
                        LOG.info("Loading nums:{}", count + 1);
                        lastTime = System.currentTimeMillis();
                    }
                    int sourceid = bufferedReaderEdge.readInt();
                    int targetid = bufferedReaderEdge.readInt();
                    float weight = bufferedReaderEdge.readFloat();
                    if (sourceid < targetid) {
                        nNeighbors[sourceid]++;
                        nNeighbors[targetid]++;
                    }
                    count++;
                }
                bufferedReaderEdge.close();
            }
            LOG.info("end read from tempfile, edge num=" + count);

        } catch (Exception e) {
            LOG.error("bufferedReaderEdge:", e);
        }

        int[] firstNeighborIndex = new int[nNodes + 1];
        nEdges = 0;
        for (i = 0; i < nNodes; i++) {
            firstNeighborIndex[i] = nEdges;
            nEdges += nNeighbors[i];
        }

        firstNeighborIndex[nNodes] = nEdges;
        int[] neighbor = new int[nEdges];
        double[] edgeWeight2 = new double[nEdges];
        Arrays.fill(nNeighbors, 0);
        try {
            LOG.info("start load edge id and weight from file");

            int count = 0;
            for (int t = 0; t < parallelNum; t++) {
                BufferedFileInput bufferedReaderEdge = new BufferedFileInput(
                        new File("edgelist.dat" + t));
                while (bufferedReaderEdge.available() > 0) {
                    if (System.currentTimeMillis() - lastTime >=
                            TimeUnit.SECONDS.toMillis(30L)) {
                        LOG.info("Loading nums:{}", count + 1);
                        lastTime = System.currentTimeMillis();
                    }
                    int sourceid = bufferedReaderEdge.readInt();
                    int targetid = bufferedReaderEdge.readInt();
                    float weight = bufferedReaderEdge.readFloat();
                    if (sourceid < targetid) {
                        j = firstNeighborIndex[sourceid] + nNeighbors[sourceid];
                        neighbor[j] = targetid;
                        edgeWeight2[j] = weight;
                        nNeighbors[sourceid]++;
                        j = firstNeighborIndex[targetid] + nNeighbors[targetid];
                        neighbor[j] = sourceid;
                        edgeWeight2[j] = weight;
                        nNeighbors[targetid]++;
                    }
                    count++;
                }
                bufferedReaderEdge.close();
            }
            LOG.info("end read from tempfile, edge num=" + count);

        } catch (Exception e) {
            LOG.error("bufferedReaderEdge:", e);
        }

        double[] nodeWeight = new double[nNodes];
        for (i = 0; i < nEdges; i++) {
            nodeWeight[neighbor[i]] += edgeWeight2[i];
        }

        Network network;
        if (modularityFunction == 1) {
            network = new Network(nNodes, firstNeighborIndex, neighbor,
                                  edgeWeight2);
        } else {
            nodeWeight = new double[nNodes];
            Arrays.fill(nodeWeight, 1);
            network = new Network(nNodes, nodeWeight, firstNeighborIndex,
                                  neighbor, edgeWeight2);
        }

        return network;
    }

    public int idGenerator() {
        return ++maxId;
    }

    public synchronized int covertId(Object hgId) {
        return this.idMap.computeIfAbsent(hgId, k -> this.idGenerator());
    }


    private void writeOutput(Clustering clustering, ComputerOutput output) {
        int i, nNodes;
        nNodes = clustering.getNNodes();
        clustering.orderClustersByNNodes();
        int nClusters = clustering.getNClusters();
        LOG.info("nClusters: {}", nClusters);
        LOG.info("nNodes: {}", nNodes);
        //BiMap<Integer, Object> biMap = this.idMap.inverse();
        try {
            List<String> idList = new ArrayList<>(this.initialCapacity);
            readIdFile(this.idFileName, nNodes, idList);

            for (i = 0; i < nNodes; i++) {
                //LOG.info("id: {}, cluster:{}", biMap.get(i),
                //         clustering.getCluster(i));
                this.vertex.id(this.context.graphFactory().
                        createId(idList.get(i)));
                this.vertex.value(new StringValue(
                        Integer.toString(clustering.getCluster(i))));
                output.write(this.vertex);
            }
        } catch (Exception e) {
            LOG.error("writeOutput:", e);
        }
    }

    private void writeIdFile(String fileName, int nNodes) throws IOException {
        BufferedWriter bufferedWriter;
        int i;
        BiMap<Integer, Object> biMap = this.idMap.inverse();
        bufferedWriter = new BufferedWriter(new FileWriter(fileName));
        for (i = 0; i < nNodes; i++) {
            bufferedWriter.write(biMap.get(i).toString());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
    }

    private void readIdFile(String fileName, int nNodes, List<String> idList)
            throws IOException {
        BufferedReader bufferedReader;
        int i;
        bufferedReader = new BufferedReader(new FileReader(fileName));
        for (i = 0; i < nNodes; i++) {
            String id = bufferedReader.readLine();
            idList.add(id);
        }
        bufferedReader.close();
    }

    /*
    private void writeOutputHg(Clustering clustering) {
        int i, nNodes;
        nNodes = clustering.getNNodes();
        clustering.orderClustersByNNodes();
        int nClusters = clustering.getNClusters();
        LOG.info("nClusters: {}", nClusters);
        LOG.info("nNodes: {}", nNodes);
        BiMap<Integer, Object> biMap = this.idMap.inverse();

        try (HugeOutput hugeOutput = new HugeOutput(config)) {
            for (i = 0; i < nNodes; i++) {
                //LOG.info("id: {}, cluster:{}", biMap.get(i),
                //         clustering.getCluster(i));
                hugeOutput.write(biMap.get(i),
                        Integer.toString(clustering.getCluster(i)));
            }
        } catch (Exception e) {
            LOG.error("writeOutputHg:", e);
        }
    }

    private void writeOutputFile(String fileName, Clustering clustering)
            throws IOException {
        BufferedWriter bufferedWriter;
        int i, nNodes;

        nNodes = clustering.getNNodes();

        clustering.orderClustersByNNodes();

        bufferedWriter = new BufferedWriter(new FileWriter(fileName));

        for (i = 0; i < nNodes; i++) {
            bufferedWriter.write(String.valueOf(i));
            bufferedWriter.write(this.delimiter);
            bufferedWriter.write(String.valueOf(clustering.getCluster(i)));
            bufferedWriter.newLine();
        }

        bufferedWriter.close();
    }

    private Network readFromHdfs(int modularityFunction) {
        int i, j, nEdges;
        List<Integer> node1 = new ArrayList<>(this.initialCapacity);
        List<Integer> node2 = new ArrayList<>(this.initialCapacity);
        List<Object> originalNode2 = new LinkedList<>();
        List<Double> edgeWeight1 = new ArrayList<>();
        int nums = 0;
        long lastTime = 0;

        StopWatch watcher = new StopWatch();
        watcher.start();
        try {
            LoaderFileGraphFetcherLocal hgFetcher =
                    new LoaderFileGraphFetcherLocal(this.config);
            Iterator<com.baidu.hugegraph.structure.graph.Edge> iterator =
                    hgFetcher.createIteratorFromEdge();

            while (iterator.hasNext()) {
                com.baidu.hugegraph.structure.graph.Edge edge = iterator.next();
                if (System.currentTimeMillis() - lastTime >=
                        TimeUnit.SECONDS.toMillis(30L)) {
                    LOG.info("Loading edge: {}, nums:{}", edge, nums + 1);
                    lastTime = System.currentTimeMillis();
                }
                Integer sourceId = this.covertId(edge.sourceId());

                node1.add(sourceId);
                originalNode2.add(edge.targetId());

                Double weight = 1.0;//ComputerOptions.DEFAULT_WEIGHT;
                if (StringUtils.isNotBlank(this.weightKey)) {
                    Double weight_ = (Double) edge.property(this.weightKey);
                    if (weight_ != null) {
                        weight = weight_;
                    }
                }
                edgeWeight1.add(weight);
                nums++;
            }

            // Covert targetId
            Iterator<Object> iterator2 = originalNode2.iterator();
            while (iterator2.hasNext()) {
                Object id = iterator2.next();
                node2.add(this.covertId(id));
                iterator2.remove();
            }
            originalNode2 = null;

            Iterator<com.baidu.hugegraph.structure.graph.Vertex> iteratorV =
                    hgFetcher.createIteratorFromVertex();
            while (iteratorV.hasNext()) {
                com.baidu.hugegraph.structure.graph.Vertex vertex =
                        iteratorV.next();
                this.covertId(vertex.id());
            }
            hgFetcher.close();
        } catch (Exception e) {
            LOG.error("readFromHG:", e);
        }
        watcher.stop();
        LOG.info("Load data complete, cost: {}, nums: {}",
                TimeUtil.readableTime(watcher.getTime()),
                nums);

        int nNodes = this.maxId + 1;
        int[] nNeighbors = new int[nNodes];
        for (i = 0; i < nums; i++) {
            if (node1.get(i) < node2.get(i)) {
                nNeighbors[node1.get(i)]++;
                nNeighbors[node2.get(i)]++;
            }
        }

        int[] firstNeighborIndex = new int[nNodes + 1];
        nEdges = 0;
        for (i = 0; i < nNodes; i++) {
            firstNeighborIndex[i] = nEdges;
            nEdges += nNeighbors[i];
        }

        firstNeighborIndex[nNodes] = nEdges;
        int[] neighbor = new int[nEdges];
        double[] edgeWeight2 = new double[nEdges];
        Arrays.fill(nNeighbors, 0);
        for (i = 0; i < nums; i++) {
            if (node1.get(i) < node2.get(i)) {
                j = firstNeighborIndex[node1.get(i)] + nNeighbors[node1.get(i)];
                neighbor[j] = node2.get(i);
                edgeWeight2[j] = edgeWeight1.get(i);
                nNeighbors[node1.get(i)]++;
                j = firstNeighborIndex[node2.get(i)] + nNeighbors[node2.get(i)];
                neighbor[j] = node1.get(i);
                edgeWeight2[j] = edgeWeight1.get(i);
                nNeighbors[node2.get(i)]++;
            }
        }

        node1 = null;
        node2 = null;
        System.gc();

        double[] nodeWeight = new double[nNodes];
        for (i = 0; i < nEdges; i++) {
            nodeWeight[neighbor[i]] += edgeWeight2[i];
        }

        Network network;
        if (modularityFunction == 1) {
            network = new Network(nNodes, firstNeighborIndex, neighbor,
                    edgeWeight2);
        } else {
            nodeWeight = new double[nNodes];
            Arrays.fill(nodeWeight, 1);
            network = new Network(nNodes, nodeWeight, firstNeighborIndex,
                    neighbor, edgeWeight2);
        }

        return network;
    }

    */
}
