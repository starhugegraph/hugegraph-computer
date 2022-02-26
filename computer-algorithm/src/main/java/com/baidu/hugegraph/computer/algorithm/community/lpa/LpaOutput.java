package com.baidu.hugegraph.computer.algorithm.community.lpa;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.define.WriteType;

public class LpaOutput extends HugeOutput {

    @Override
    public void prepareSchema(HugeGraph graph) {
        graph.schema().propertyKey(this.name())
                             .asText()
                             .writeType(WriteType.OLAP_COMMON)
                             .ifNotExist()
                             .create();
    }

    @Override
    public HugeVertex constructHugeVertex(Vertex vertex) {
        /*HugeVertex hugeVertex = new HugeVertex(
                this.graph(), IdGenerator.of(vertex.id().asObject()),
                this.graph().vertexLabel(vertex.label()));*/
        GraphTransaction gtx = Whitebox.invoke(this.graph().getClass(),
                "graphTransaction", this.graph());
        HugeVertex hugeVertex = HugeVertex.create(gtx,
                IdGenerator.of(vertex.id().asObject()),
                VertexLabel.OLAP_VL);
        hugeVertex.property(this.name(), vertex.value().toString());
        return hugeVertex;
    }
}
