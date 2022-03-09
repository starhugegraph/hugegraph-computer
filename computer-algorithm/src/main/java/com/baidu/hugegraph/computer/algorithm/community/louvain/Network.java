package com.baidu.hugegraph.computer.algorithm.community.louvain;

import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.Random;

public class Network implements Cloneable, Serializable {
    private static final Logger LOG = Log.logger(Network.class);

    private static final long serialVersionUID = 1;
    private int nNodes;
    private int[] firstNeighborIndex;
    private int[] neighbor;
    private double[] edgeWeight;
    private double[] nodeWeight;
    private int nClusters;
    private int[] cluster;
    private double[] clusterWeight;
    private int[] nNodesPerCluster;
    private int[][] nodePerCluster;
    private boolean clusteringStatsAvailable;
    public Network(int nNodes, int[] firstNeighborIndex, int[] neighbor,
                   double[] edgeWeight, double[] nodeWeight) {
        this(nNodes, firstNeighborIndex, neighbor, edgeWeight,
                nodeWeight, null);
    }

    public Network(int nNodes, int[] firstNeighborIndex, int[] neighbor,
                   double[] edgeWeight, double[] nodeWeight, int[] cluster) {
        int i, nEdges;
        this.nNodes = nNodes;
        this.firstNeighborIndex = firstNeighborIndex;
        this.neighbor = neighbor;

        if (edgeWeight == null) {
            nEdges = neighbor.length;
            this.edgeWeight = new double[nEdges];
            for (i = 0; i < nEdges; i++)
                this.edgeWeight[i] = 1;
        } else
            this.edgeWeight = edgeWeight;

        if (nodeWeight == null) {
            this.nodeWeight = new double[nNodes];
            for (i = 0; i < nNodes; i++)
                this.nodeWeight[i] = 1;
        } else
            this.nodeWeight = nodeWeight;
    }

    public int getNNodes() {
        return nNodes;
    }

    public int getNEdges() {
        return neighbor.length;
    }

    public double getTotalEdgeWeight() {
        double totalEdgeWeight;
        int i;
        totalEdgeWeight = 0;
        for (i = 0; i < neighbor.length; i++)
            totalEdgeWeight += edgeWeight[i];
        return totalEdgeWeight;
    }

    public double[] getEdgeWeights() {
        return edgeWeight;
    }

    public double[] getNodeWeights() {
        return nodeWeight;
    }

    public int getNClusters() {
        return nClusters;
    }

    public int[] getClusters() {
        return cluster;
    }

    public void initSingletonClusters() {
        int i;
        nClusters = nNodes;
        cluster = new int[nNodes];
        for (i = 0; i < nNodes; i++)
            cluster[i] = i;
        deleteClusteringStats();
    }

    public void mergeClusters(int[] newCluster) {
        int i, j, k;
        if (cluster == null)
            return;

        i = 0;
        for (j = 0; j < nNodes; j++) {
            k = newCluster[cluster[j]];
            if (k > i)
                i = k;
            cluster[j] = k;
        }
        nClusters = i + 1;
        deleteClusteringStats();
    }

    public Network getReducedNetwork() {
        double[] reducedNetworkEdgeWeight1, reducedNetworkEdgeWeight2;
        int i, j, k, l, m, reducedNetworkNEdges1, reducedNetworkNEdges2;
        int[] reducedNetworkNeighbor1, reducedNetworkNeighbor2;
        Network reducedNetwork;
        if (cluster == null)
            return null;

        if (!clusteringStatsAvailable)
            calcClusteringStats();

        reducedNetwork = new Network();
        reducedNetwork.nNodes = nClusters;
        reducedNetwork.firstNeighborIndex = new int[nClusters + 1];
        reducedNetwork.nodeWeight = new double[nClusters];
        reducedNetworkNeighbor1 = new int[neighbor.length];
        reducedNetworkEdgeWeight1 = new double[edgeWeight.length];
        reducedNetworkNeighbor2 = new int[nClusters - 1];
        reducedNetworkEdgeWeight2 = new double[nClusters];
        reducedNetworkNEdges1 = 0;

        for (i = 0; i < nClusters; i++) {
            reducedNetworkNEdges2 = 0;
            for (j = 0; j < nodePerCluster[i].length; j++) {
                k = nodePerCluster[i][j]; // k是簇i中第j个节点的id
                for (l = firstNeighborIndex[k]; l < firstNeighborIndex[k + 1];
                     l++) {
                    m = cluster[neighbor[l]]; // m是k的在l位置的邻居节点所属的簇id
                    if (m != i) {
                        if (reducedNetworkEdgeWeight2[m] == 0) {
                            reducedNetworkNeighbor2[reducedNetworkNEdges2] = m;
                            reducedNetworkNEdges2++;
                        }
                        reducedNetworkEdgeWeight2[m] += edgeWeight[l];
                    }
                }
                reducedNetwork.nodeWeight[i] += nodeWeight[k];
            }

            for (j = 0; j < reducedNetworkNEdges2; j++) {
                reducedNetworkNeighbor1[reducedNetworkNEdges1 + j] =
                        reducedNetworkNeighbor2[j];
                reducedNetworkEdgeWeight1[reducedNetworkNEdges1 + j] =
                        reducedNetworkEdgeWeight2[reducedNetworkNeighbor2[j]];
                reducedNetworkEdgeWeight2[reducedNetworkNeighbor2[j]] = 0;
            }
            reducedNetworkNEdges1 += reducedNetworkNEdges2;
            reducedNetwork.firstNeighborIndex[i + 1] = reducedNetworkNEdges1;
        }
        reducedNetwork.neighbor = new int[reducedNetworkNEdges1];
        reducedNetwork.edgeWeight = new double[reducedNetworkNEdges1];
        System.arraycopy(reducedNetworkNeighbor1, 0,
                reducedNetwork.neighbor, 0, reducedNetworkNEdges1);
        System.arraycopy(reducedNetworkEdgeWeight1, 0,
                reducedNetwork.edgeWeight, 0, reducedNetworkNEdges1);
        return reducedNetwork;
    }

    public double calcQualityFunction(double resolution) {
        double qualityFunction, totalEdgeWeight;
        int i, j, k;
        if (cluster == null)
            return Double.NaN;
        if (!clusteringStatsAvailable)
            calcClusteringStats();
        qualityFunction = 0;
        totalEdgeWeight = 0;
        for (i = 0; i < nNodes; i++) {
            j = cluster[i];
            for (k = firstNeighborIndex[i];k < firstNeighborIndex[i + 1];k++) {
                if (cluster[neighbor[k]] == j)
                    qualityFunction += edgeWeight[k];
                totalEdgeWeight += edgeWeight[k];
            }
        }

        for (i = 0; i < nClusters; i++)
            qualityFunction -= clusterWeight[i] * clusterWeight[i] * resolution;

        qualityFunction /= totalEdgeWeight;
        return qualityFunction;

    }

    public boolean runLocalMovingAlgorithm(double resolution) {
        return runLocalMovingAlgorithm(resolution, new Random());
    }

    public boolean runLocalMovingAlgorithm(double resolution, Random random) {
        boolean update;
        double maxQualityFunction, qualityFunction;
        double[] clusterWeight, edgeWeightPerCluster;
        int bestCluster, i, j, k, l, nNeighboringClusters,
                nStableNodes, nUnusedClusters;
        int[] neighboringCluster, newCluster, nNodesPerCluster,
                nodeOrder, unusedCluster;
        if ((cluster == null) || (nNodes == 1))
            return false;

        update = false;
        clusterWeight = new double[nNodes];
        nNodesPerCluster = new int[nNodes];

        for (i = 0; i < nNodes; i++) {
            clusterWeight[cluster[i]] += nodeWeight[i];
            nNodesPerCluster[cluster[i]]++;
        }

        nUnusedClusters = 0;
        unusedCluster = new int[nNodes];
        for (i = 0; i < nNodes; i++)
            if (nNodesPerCluster[i] == 0) {
                unusedCluster[nUnusedClusters] = i;
                nUnusedClusters++;
            }

        nodeOrder = new int[nNodes];
        for (i = 0; i < nNodes; i++)
            nodeOrder[i] = i;

        for (i = 0; i < nNodes; i++) {
            j = random.nextInt(nNodes);
            k = nodeOrder[i];
            nodeOrder[i] = nodeOrder[j];
            nodeOrder[j] = k;
        }

        edgeWeightPerCluster = new double[nNodes];
        neighboringCluster = new int[nNodes - 1];
        nStableNodes = 0;
        i = 0;

        do {
            j = nodeOrder[i];
            nNeighboringClusters = 0;
            for (k = firstNeighborIndex[j];k < firstNeighborIndex[j + 1];k++) {
                l = cluster[neighbor[k]];
                if (edgeWeightPerCluster[l] == 0) {
                    neighboringCluster[nNeighboringClusters] = l;
                    nNeighboringClusters++;
                }
                edgeWeightPerCluster[l] += edgeWeight[k];
            }

            clusterWeight[cluster[j]] -= nodeWeight[j];
            nNodesPerCluster[cluster[j]]--;
            if (nNodesPerCluster[cluster[j]] == 0) {
                unusedCluster[nUnusedClusters] = cluster[j];
                nUnusedClusters++;
            }

            bestCluster = -1;
            maxQualityFunction = 0;
            for (k = 0; k < nNeighboringClusters; k++) {
                l = neighboringCluster[k];
                qualityFunction = edgeWeightPerCluster[l] -
                        nodeWeight[j] * clusterWeight[l] * resolution;
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

            clusterWeight[bestCluster] += nodeWeight[j];
            nNodesPerCluster[bestCluster]++;
            if (bestCluster == cluster[j])
                nStableNodes++;
            else {
                cluster[j] = bestCluster;
                nStableNodes = 1;
                update = true;
            }
            i = (i < nNodes - 1) ? (i + 1) : 0;

        } while (nStableNodes < nNodes); // 优化步骤是直到所有的点都稳定下来才结束

        newCluster = new int[nNodes];
        nClusters = 0;
        for (i = 0; i < nNodes; i++)
            if (nNodesPerCluster[i] > 0) {
                newCluster[i] = nClusters;
                nClusters++;
            }

        for (i = 0; i < nNodes; i++)
            cluster[i] = newCluster[cluster[i]];

        deleteClusteringStats();
        return update;
    }

    public boolean runLouvainAlgorithm(double resolution) {
        return runLouvainAlgorithm(resolution, new Random());
    }

    public boolean runLouvainAlgorithm(double resolution, Random random) {
        boolean update, update2;
        Network reducedNetwork;
        if ((cluster == null) || (nNodes == 1))
            return false;

        update = runLocalMovingAlgorithm(resolution, random);

        LOG.info("nClusters:{},nNodes:{}",nClusters,nNodes);
        if (nClusters < nNodes) {
            reducedNetwork = getReducedNetwork();
            reducedNetwork.initSingletonClusters();
            update2 = reducedNetwork.runLouvainAlgorithm(resolution, random);
            if (update2) {
                update = true;
                mergeClusters(reducedNetwork.getClusters());
            }
        }
        deleteClusteringStats();
        return update;
    }

    private Network() {
    }

    private void calcClusteringStats() {
        int i, j;
        clusterWeight = new double[nClusters];
        nNodesPerCluster = new int[nClusters];
        nodePerCluster = new int[nClusters][];
        for (i = 0; i < nNodes; i++) {
            clusterWeight[cluster[i]] += nodeWeight[i];
            nNodesPerCluster[cluster[i]]++;
        }

        for (i = 0; i < nClusters; i++) {
            nodePerCluster[i] = new int[nNodesPerCluster[i]];
            nNodesPerCluster[i] = 0;
        }

        for (i = 0; i < nNodes; i++) {
            j = cluster[i];
            nodePerCluster[j][nNodesPerCluster[j]] = i;
            nNodesPerCluster[j]++;
        }

        clusteringStatsAvailable = true;
    }

    private void deleteClusteringStats() {
        clusterWeight = null;
        nNodesPerCluster = null;
        nodePerCluster = null;
        clusteringStatsAvailable = false;
    }
}