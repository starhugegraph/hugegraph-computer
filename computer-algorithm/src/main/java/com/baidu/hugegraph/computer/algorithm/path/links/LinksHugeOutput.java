package com.baidu.hugegraph.computer.algorithm.path.links;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.define.WriteType;

import java.util.ArrayList;
import java.util.List;

public class LinksHugeOutput extends HugeOutput {

    @Override
    public void prepareSchema(HugeGraph graph) {
        graph.schema().propertyKey(this.name())
                             .asText()
                             .writeType(WriteType.OLAP_COMMON)
                             .valueList()
                             .ifNotExist()
                             .create();
    }

    @Override
    public HugeVertex constructHugeVertex(
            com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
        /*HugeVertex hugeVertex = new HugeVertex(
                this.graph(), IdGenerator.of(vertex.id().asObject()),
                this.graph().vertexLabel(vertex.label()));*/
        GraphTransaction gtx = Whitebox.invoke(this.graph().getClass(),
                "graphTransaction", this.graph());
        HugeVertex hugeVertex = HugeVertex.create(gtx,
                IdGenerator.of(vertex.id().asObject()),
                VertexLabel.OLAP_VL);

        LinksValue value = vertex.value();
        List<String> propValue = new ArrayList<>();
        for (int i = 0; i < value.size(); i++) {
            propValue.add(value.values().get(i).toString());
        }

        hugeVertex.property(this.name(), propValue);
        return hugeVertex;
    }
}
