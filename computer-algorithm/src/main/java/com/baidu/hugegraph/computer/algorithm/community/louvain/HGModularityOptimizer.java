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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.NotSupportedException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

//import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.algorithm.community.louvain.hg.input.GraphFetcher;
import com.baidu.hugegraph.computer.algorithm.community.louvain.hg.HugeOutput;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
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
    private final BiMap<Object, Integer> idMap;
    private final String weightKey;
    private final String delimiter;
    private int maxId;

    public static final String OPTION_CAPACITY = "louvain.capacity";
    public static final String OPTION_WEIGHTKEY = "louvain.weightkey";
    public static final String OPTION_DELIMITER = "louvain.delimiter";
    public static final String OPTION_MODULARITY = "louvain.modularity";
    public static final String OPTION_RESOLUTION = "louvain.resolution";
    public static final String OPTION_RANDOMSTART = "louvain.randomstart";
    public static final String OPTION_ITERATIONS = "louvain.iterations";
    public static final String OPTION_RANDOMSEED = "louvain.randomseed";
    public static final String OPTION_INPUTTYPE = "louvain.inputtype";
    public static final String OPTION_INPUTPATH = "louvain.inputpath";
    public static final String OPTION_OUTPUTTYPE = "louvain.outputtype";
    public static final String OPTION_OUTPUTPATH = "louvain.outputpath";

    public HGModularityOptimizer(Config config) {
        this.config = config;
        this.maxId = -1;
        this.initialCapacity = config.getInt(OPTION_CAPACITY,50000000);
        System.out.println("initialCapacity:" + this.initialCapacity);
        this.idMap = HashBiMap.create(this.initialCapacity);
        this.weightKey = config.getString(OPTION_WEIGHTKEY,"");
        this.delimiter = config.getString(OPTION_DELIMITER," ");
    }

    public void runAlgorithm() throws IOException {
        int algorithm = ALGORITHM;
        int modularityFunction = config.getInt(OPTION_MODULARITY,1);
        double resolution = config.getDouble(OPTION_RESOLUTION,1.0);
        int nRandomStarts = config.getInt(OPTION_RANDOMSTART,1);
        int nIterations = config.getInt(OPTION_ITERATIONS,10);
        long randomSeed = config.getLong(OPTION_RANDOMSEED,100);

        VOSClusteringTechnique vOSClusteringTechnique;
        double modularity, maxModularity, resolution2;
        int i, j;

        LOG.info("Modularity Optimizer version 1.3.0 by Ludo Waltman and " +
                 "Nees Jan van Eck");

        LOG.info("Start input data...");
        StopWatch watcher = new StopWatch();
        watcher.start();

        String inputType = config.getString(OPTION_INPUTTYPE,"hugegraph");
        Network network;
        switch (inputType) {
            case "hugegraph":
                network = this.readFromHG(modularityFunction);
                break;
            case "file":
                String inputFilePath = config.getString(OPTION_INPUTPATH,"");
                network = this.readInputFile(inputFilePath, modularityFunction);
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
        }
        watcher.stop();
        LOG.info("End output, cost:{}",
                 TimeUtil.readableTime(watcher.getTime()));
    }

    private Network readFromHG(int modularityFunction) {
        int i, j, nEdges;
        List<Integer> node1 = new ArrayList<>(this.initialCapacity);
        List<Integer> node2 = new ArrayList<>(this.initialCapacity);
        List<Object> originalNode2 = new LinkedList<>();
        List<Double> edgeWeight1 = new ArrayList<>();
        int nums = 0;
        long lastTime = 0;

        StopWatch watcher = new StopWatch();
        watcher.start();
        try (GraphFetcher hgFetcher = new GraphFetcher(this.config)) {
            Iterator<Edge> iterator = hgFetcher.createIteratorFromEdge();

            while (iterator.hasNext()) {
                Edge edge = iterator.next();
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


            Iterator<Vertex> iteratorV = hgFetcher.createIteratorFromVertex();
            while (iteratorV.hasNext()) {
                Vertex vertex = iteratorV.next();
                this.covertId(vertex.id());
            }
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

    private void writeOutputHg(Clustering clustering) {
        int i, nNodes;
        nNodes = clustering.getNNodes();
        clustering.orderClustersByNNodes();
        int nClusters = clustering.getNClusters();
        LOG.info("nClusters: {}", nClusters);
        BiMap<Integer, Object> biMap = this.idMap.inverse();

        try (HugeOutput hugeOutput = new HugeOutput(config)) {
            for (i = 0; i < nNodes; i++) {
                //LOG.info("id: {}, cluster:{}", biMap.get(i),
                //         clustering.getCluster(i));
                hugeOutput.write(biMap.get(i),
                                 Integer.toString(clustering.getCluster(i)));
            }
        }
    }

    public int idGenerator() {
        return ++maxId;
    }

    public int covertId(Object hgId) {
        return this.idMap.computeIfAbsent(hgId, k -> this.idGenerator());
    }


    private Network readInputFile(String fileName, int modularityFunction)
            throws IOException {
        BufferedReader bufferedReader;
        double[] edgeWeight1, edgeWeight2, nodeWeight;
        int i, j, nEdges, nLines, nNodes;
        int[] firstNeighborIndex, neighbor, nNeighbors, node1, node2;
        Network network;
        String[] splittedLine;

        bufferedReader = new BufferedReader(new FileReader(fileName));

        nLines = 0;
        while (bufferedReader.readLine() != null)
            nLines++;

        bufferedReader.close();

        bufferedReader = new BufferedReader(new FileReader(fileName));

        node1 = new int[nLines];
        node2 = new int[nLines];
        edgeWeight1 = new double[nLines];
        i = -1;
        long lastTime = 0;
        for (j = 0; j < nLines; j++) {
            if (System.currentTimeMillis() - lastTime >=
                    TimeUnit.SECONDS.toMillis(30L)) {
                LOG.info("Loading edge nums:{}", j + 1);
                lastTime = System.currentTimeMillis();
            }

            splittedLine = StringUtils.split(bufferedReader.readLine(),
                                             this.delimiter);
            node1[j] = Integer.parseInt(splittedLine[0]);
            if (node1[j] > i)
                i = node1[j];
            node2[j] = Integer.parseInt(splittedLine[1]);
            if (node2[j] > i)
                i = node2[j];
            edgeWeight1[j] = (splittedLine.length > 2) ?
                    Double.parseDouble(splittedLine[2]) :
                    1.0;//ComputerOptions.DEFAULT_WEIGHT;
        }
        nNodes = i + 1;

        bufferedReader.close();

        nNeighbors = new int[nNodes];
        for (i = 0; i < nLines; i++)
            if (node1[i] < node2[i]) {
                nNeighbors[node1[i]]++;
                nNeighbors[node2[i]]++;
            }

        firstNeighborIndex = new int[nNodes + 1];
        nEdges = 0;
        for (i = 0; i < nNodes; i++) {
            firstNeighborIndex[i] = nEdges;
            nEdges += nNeighbors[i];
        }
        firstNeighborIndex[nNodes] = nEdges;

        neighbor = new int[nEdges];
        edgeWeight2 = new double[nEdges];
        Arrays.fill(nNeighbors, 0);
        for (i = 0; i < nLines; i++)
            if (node1[i] < node2[i]) {
                j = firstNeighborIndex[node1[i]] + nNeighbors[node1[i]];
                neighbor[j] = node2[i];
                edgeWeight2[j] = edgeWeight1[i];
                nNeighbors[node1[i]]++;
                j = firstNeighborIndex[node2[i]] + nNeighbors[node2[i]];
                neighbor[j] = node1[i];
                edgeWeight2[j] = edgeWeight1[i];
                nNeighbors[node2[i]]++;
            }

        if (modularityFunction == 1)
            network = new Network(nNodes, firstNeighborIndex,
                    neighbor, edgeWeight2);
        else {
            nodeWeight = new double[nNodes];
            Arrays.fill(nodeWeight, 1);
            network = new Network(nNodes, nodeWeight, firstNeighborIndex,
                    neighbor, edgeWeight2);
        }

        return network;
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
}
