package com.baidu.hugegraph.computer.algorithm.community.cc;

import com.baidu.hugegraph.computer.core.output.hdfs.HdfsOutput;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class ClusteringCoefficientOutputHdfs extends HdfsOutput {
    @Override
    protected String constructValueString(Vertex vertex) {
        float triangle = ((ClusteringCoefficientValue) vertex.value()).count();
        int degree = ((ClusteringCoefficientValue) vertex.value())
                                                  .idSet().size();
        double cc = 0.0;
        if (degree > 1) {
            cc = 2 * triangle / degree / (degree - 1);
        }
        return String.valueOf(cc);
    }
}
