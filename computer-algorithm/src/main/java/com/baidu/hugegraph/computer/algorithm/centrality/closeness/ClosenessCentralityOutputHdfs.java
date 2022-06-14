package com.baidu.hugegraph.computer.algorithm.centrality.closeness;

import com.baidu.hugegraph.computer.core.output.hdfs.HdfsOutput;
import java.util.Map;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class ClosenessCentralityOutputHdfs extends HdfsOutput {

    @Override
    protected String constructValueString(Vertex vertex) {
        ClosenessValue localValue = vertex.value();
        // Cumulative distance
        double centrality = 0;
        for (Map.Entry<Id, DoubleValue> entry : localValue.entrySet()) {
            centrality += 1.0D / entry.getValue().value();
        }
        return String.valueOf(centrality);
    }
}
