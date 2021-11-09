package com.baidu.hugegraph.computer.core.output.hdfs;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class FilterBlankValueHdfsOutput extends HdfsOutput {

    @Override
    protected boolean filter(Vertex vertex) {
        return vertex.value() != null &&
               StringUtils.isNoneEmpty(vertex.value().string());
    }
}
