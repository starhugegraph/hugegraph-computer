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

package com.baidu.hugegraph.computer.algorithm.community.louvain;

import java.util.Random;

public class VOSClusteringTechnique {
    protected Network network;
    protected Clustering clustering;
    protected double resolution;

    public VOSClusteringTechnique(Network network, double resolution) {
        this.network = network;
        clustering = new Clustering(network.nNodes);
        clustering.initSingletonClusters();
        this.resolution = resolution;
    }

    public VOSClusteringTechnique(Network network,
                                  Clustering clustering, double resolution) {
        this.network = network;
        this.clustering = clustering;
        this.resolution = resolution;
    }

    public Network getNetwork() {
        return network;
    }

    public Clustering getClustering() {
        return clustering;
    }

    public double getResolution() {
        return resolution;
    }

    public void setNetwork(Network network) {
        this.network = network;
    }

    public void setClustering(Clustering clustering) {
        this.clustering = clustering;
    }

    public void setResolution(double resolution) {
        this.resolution = resolution;
    }

    public double calcQualityFunction() {
        double qualityFunction;
        double[] clusterWeight;
        int i, j, k;

        qualityFunction = 0;

        for (i = 0; i < network.nNodes; i++) {
            j = clustering.cluster[i];
            for (k = network.firstNeighborIndex[i];
                 k < network.firstNeighborIndex[i + 1]; k++)
                if (clustering.cluster[network.neighbor[k]] == j)
                    qualityFunction += network.edgeWeight[k];
        }
        qualityFunction += network.totalEdgeWeightSelfLinks;

        clusterWeight = new double[clustering.nClusters];
        for (i = 0; i < network.nNodes; i++)
            clusterWeight[clustering.cluster[i]] += network.nodeWeight[i];
        for (i = 0; i < clustering.nClusters; i++)
            qualityFunction -= clusterWeight[i] * clusterWeight[i] * resolution;

        qualityFunction /= 2 * network.getTotalEdgeWeight() +
                network.totalEdgeWeightSelfLinks;

        return qualityFunction;
    }

    public boolean runLocalMovingAlgorithm() {
        return runLocalMovingAlgorithm(new Random());
    }

    public boolean runLocalMovingAlgorithm(Random random) {
        boolean update;
        double maxQualityFunction, qualityFunction;
        double[] clusterWeight, edgeWeightPerCluster;
        int bestCluster, i, j, k, l, nNeighboringClusters,
                nStableNodes, nUnusedClusters;
        int[] neighboringCluster, newCluster, nNodesPerCluster,
                nodePermutation, unusedCluster;

        if (network.nNodes == 1)
            return false;

        update = false;

        clusterWeight = new double[network.nNodes];
        nNodesPerCluster = new int[network.nNodes];
        for (i = 0; i < network.nNodes; i++) {
            clusterWeight[clustering.cluster[i]] += network.nodeWeight[i];
            nNodesPerCluster[clustering.cluster[i]]++;
        }

        nUnusedClusters = 0;
        unusedCluster = new int[network.nNodes];
        for (i = 0; i < network.nNodes; i++)
            if (nNodesPerCluster[i] == 0) {
                unusedCluster[nUnusedClusters] = i;
                nUnusedClusters++;
            }

        nodePermutation = Arrays2.generateRandomPermutation(
                network.nNodes, random);

        edgeWeightPerCluster = new double[network.nNodes];
        neighboringCluster = new int[network.nNodes - 1];
        nStableNodes = 0;
        i = 0;
        do {
            j = nodePermutation[i];

            nNeighboringClusters = 0;
            for (k = network.firstNeighborIndex[j];
                 k < network.firstNeighborIndex[j + 1]; k++) {
                l = clustering.cluster[network.neighbor[k]];
                if (edgeWeightPerCluster[l] == 0) {
                    neighboringCluster[nNeighboringClusters] = l;
                    nNeighboringClusters++;
                }
                edgeWeightPerCluster[l] += network.edgeWeight[k];
            }

            clusterWeight[clustering.cluster[j]] -= network.nodeWeight[j];
            nNodesPerCluster[clustering.cluster[j]]--;
            if (nNodesPerCluster[clustering.cluster[j]] == 0) {
                unusedCluster[nUnusedClusters] = clustering.cluster[j];
                nUnusedClusters++;
            }

            bestCluster = -1;
            maxQualityFunction = 0;
            for (k = 0; k < nNeighboringClusters; k++) {
                l = neighboringCluster[k];
                qualityFunction = edgeWeightPerCluster[l] -
                        network.nodeWeight[j] * clusterWeight[l] * resolution;
                if ((qualityFunction > maxQualityFunction) ||
                        ((qualityFunction == maxQualityFunction) &&
                                (l < bestCluster))) {
                    bestCluster = l;
                    maxQualityFunction = qualityFunction;
                }
                edgeWeightPerCluster[l] = 0;
            }
            if (maxQualityFunction == 0) {
                bestCluster = unusedCluster[nUnusedClusters - 1];
                nUnusedClusters--;
            }

            clusterWeight[bestCluster] += network.nodeWeight[j];
            nNodesPerCluster[bestCluster]++;
            if (bestCluster == clustering.cluster[j])
                nStableNodes++;
            else {
                clustering.cluster[j] = bestCluster;
                nStableNodes = 1;
                update = true;
            }

            i = (i < network.nNodes - 1) ? (i + 1) : 0;
        }
        while (nStableNodes < network.nNodes);

        newCluster = new int[network.nNodes];
        clustering.nClusters = 0;
        for (i = 0; i < network.nNodes; i++)
            if (nNodesPerCluster[i] > 0) {
                newCluster[i] = clustering.nClusters;
                clustering.nClusters++;
            }
        for (i = 0; i < network.nNodes; i++)
            clustering.cluster[i] = newCluster[clustering.cluster[i]];

        return update;
    }

    public boolean runLouvainAlgorithm() {
        return runLouvainAlgorithm(new Random());
    }

    public boolean runLouvainAlgorithm(Random random) {
        boolean update, update2;
        VOSClusteringTechnique vOSClusteringTechnique;

        if (network.nNodes == 1)
            return false;

        update = runLocalMovingAlgorithm(random);

        if (clustering.nClusters < network.nNodes) {
            vOSClusteringTechnique = new VOSClusteringTechnique(
                    network.createReducedNetwork(clustering), resolution);

            update2 = vOSClusteringTechnique.runLouvainAlgorithm(random);

            if (update2) {
                update = true;

                clustering.mergeClusters(vOSClusteringTechnique.clustering);
            }
        }

        return update;
    }

    public boolean runIteratedLouvainAlgorithm(int maxNIterations) {
        return runIteratedLouvainAlgorithm(maxNIterations, new Random());
    }

    public boolean runIteratedLouvainAlgorithm(
            int maxNIterations, Random random) {
        boolean update;
        int i;

        i = 0;
        do {
            update = runLouvainAlgorithm(random);
            i++;
        }
        while ((i < maxNIterations) && update);
        return ((i > 1) || update);
    }

    public boolean runLouvainAlgorithmWithMultilevelRefinement() {
        return runLouvainAlgorithmWithMultilevelRefinement(new Random());
    }

    public boolean runLouvainAlgorithmWithMultilevelRefinement(Random random) {
        boolean update, update2;
        VOSClusteringTechnique vOSClusteringTechnique;

        if (network.nNodes == 1)
            return false;

        update = runLocalMovingAlgorithm(random);

        if (clustering.nClusters < network.nNodes) {
            vOSClusteringTechnique = new VOSClusteringTechnique(
                    network.createReducedNetwork(clustering), resolution);

            update2 = vOSClusteringTechnique.
                    runLouvainAlgorithmWithMultilevelRefinement(random);

            if (update2) {
                update = true;

                clustering.mergeClusters(vOSClusteringTechnique.clustering);

                runLocalMovingAlgorithm(random);
            }
        }

        return update;
    }

    public boolean runIteratedLouvainAlgorithmWithMultilevelRefinement(
            int maxNIterations) {
        return runIteratedLouvainAlgorithmWithMultilevelRefinement(
                maxNIterations, new Random());
    }

    public boolean runIteratedLouvainAlgorithmWithMultilevelRefinement(
            int maxNIterations, Random random) {
        boolean update;
        int i;

        i = 0;
        do {
            update = runLouvainAlgorithmWithMultilevelRefinement(random);
            i++;
        }
        while ((i < maxNIterations) && update);
        return ((i > 1) || update);
    }

    public boolean runSmartLocalMovingAlgorithm() {
        return runSmartLocalMovingAlgorithm(new Random());
    }

    public boolean runSmartLocalMovingAlgorithm(Random random) {
        boolean update;
        int i, j, k;
        int[] nNodesPerClusterReducedNetwork;
        int[][] nodePerCluster;
        Network[] subnetwork;
        VOSClusteringTechnique vOSClusteringTechnique;

        if (network.nNodes == 1)
            return false;

        update = runLocalMovingAlgorithm(random);

        if (clustering.nClusters < network.nNodes) {
            subnetwork = network.createSubnetworks(clustering);

            nodePerCluster = clustering.getNodesPerCluster();

            clustering.nClusters = 0;
            nNodesPerClusterReducedNetwork = new int[subnetwork.length];
            for (i = 0; i < subnetwork.length; i++) {
                vOSClusteringTechnique = new
                        VOSClusteringTechnique(subnetwork[i], resolution);

                vOSClusteringTechnique.runLocalMovingAlgorithm(random);

                for (j = 0; j < subnetwork[i].nNodes; j++)
                    clustering.cluster[nodePerCluster[i][j]] =
                            clustering.nClusters + vOSClusteringTechnique.
                                    clustering.cluster[j];
                clustering.nClusters +=
                        vOSClusteringTechnique.clustering.nClusters;
                nNodesPerClusterReducedNetwork[i] =
                        vOSClusteringTechnique.clustering.nClusters;
            }

            vOSClusteringTechnique = new VOSClusteringTechnique(
                    network.createReducedNetwork(clustering), resolution);

            i = 0;
            for (j = 0; j < nNodesPerClusterReducedNetwork.length; j++)
                for (k = 0; k < nNodesPerClusterReducedNetwork[j]; k++) {
                    vOSClusteringTechnique.clustering.cluster[i] = j;
                    i++;
                }
            vOSClusteringTechnique.clustering.nClusters =
                    nNodesPerClusterReducedNetwork.length;

            update |= vOSClusteringTechnique.
                    runSmartLocalMovingAlgorithm(random);

            clustering.mergeClusters(vOSClusteringTechnique.clustering);
        }

        return update;
    }

    public boolean runIteratedSmartLocalMovingAlgorithm(int nIterations) {
        return runIteratedSmartLocalMovingAlgorithm(nIterations, new Random());
    }

    public boolean runIteratedSmartLocalMovingAlgorithm(int nIterations,
                                                        Random random) {
        boolean update;
        int i;

        update = false;
        for (i = 0; i < nIterations; i++)
            update |= runSmartLocalMovingAlgorithm(random);
        return update;
    }

    public int removeCluster(int cluster) {
        double maxQualityFunction, qualityFunction;
        double[] clusterWeight, totalEdgeWeightPerCluster;
        int i, j;

        clusterWeight = new double[clustering.nClusters];
        totalEdgeWeightPerCluster = new double[clustering.nClusters];
        for (i = 0; i < network.nNodes; i++) {
            clusterWeight[clustering.cluster[i]] += network.nodeWeight[i];
            if (clustering.cluster[i] == cluster)
                for (j = network.firstNeighborIndex[i];
                     j < network.firstNeighborIndex[i + 1]; j++)
                    totalEdgeWeightPerCluster[clustering.cluster
                            [network.neighbor[j]]] += network.edgeWeight[j];
        }

        i = -1;
        maxQualityFunction = 0;
        for (j = 0; j < clustering.nClusters; j++)
            if ((j != cluster) && (clusterWeight[j] > 0)) {
                qualityFunction = totalEdgeWeightPerCluster[j] /
                        clusterWeight[j];
                if (qualityFunction > maxQualityFunction) {
                    i = j;
                    maxQualityFunction = qualityFunction;
                }
            }

        if (i >= 0) {
            for (j = 0; j < network.nNodes; j++)
                if (clustering.cluster[j] == cluster)
                    clustering.cluster[j] = i;
            if (cluster == clustering.nClusters - 1)
                clustering.nClusters = Arrays2.calcMaximum(
                        clustering.cluster) + 1;
        }

        return i;
    }

    public void removeSmallClusters(int minNNodesPerCluster) {
        int i, j, k;
        int[] nNodesPerCluster;
        VOSClusteringTechnique vOSClusteringTechnique;

        vOSClusteringTechnique = new VOSClusteringTechnique(
                network.createReducedNetwork(clustering), resolution);

        nNodesPerCluster = clustering.getNNodesPerCluster();

        do {
            i = -1;
            j = minNNodesPerCluster;
            for (k = 0; k < vOSClusteringTechnique.clustering.nClusters; k++)
                if ((nNodesPerCluster[k] > 0) && (nNodesPerCluster[k] < j)) {
                    i = k;
                    j = nNodesPerCluster[k];
                }

            if (i >= 0) {
                j = vOSClusteringTechnique.removeCluster(i);
                if (j >= 0)
                    nNodesPerCluster[j] += nNodesPerCluster[i];
                nNodesPerCluster[i] = 0;
            }
        }
        while (i >= 0);

        clustering.mergeClusters(vOSClusteringTechnique.clustering);
    }
}
