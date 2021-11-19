package com.baidu.hugegraph.computer.algorithm.path.paths;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import java.util.Iterator;


public class PathFinding implements Computation<DoubleValue> {
    private String targetString;
    public static final String OPTION_PATHFINDING_TARGET =
                               "pathfinding.targetstring";

    @Override
    public String name() {
        return "rings";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void init(Config config) {
        this.targetString = config.getString(OPTION_PATHFINDING_TARGET, "");
    }

    @Override
    public void close(Config config) {
        // pass
    }

    
    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new DoubleValue(-1.0D));
        if (!vertex.id().toString().equals(targetString)) {
            return;
        }
        if (vertex.edges().size() == 0) {
            vertex.inactivate();
            return;
        }

        // Init path
        DoubleValue currValue = new DoubleValue(0.0D);
        vertex.value(currValue);

        for (Edge edge : vertex.edges()) {
            if (edge.properties().get("inv") == null) {
                continue;
            }
            DoubleValue weight = edge.properties().get("weight");
            if (weight == null) {
                weight = new DoubleValue(1.0D);
            }
            context.sendMessage(edge.targetId(), weight);
        }
        vertex.inactivate();
    }
    
    @Override
    public void compute(ComputationContext context, Vertex vertex,
    Iterator<DoubleValue> messages) {
        DoubleValue message = Combiner.combineAll(context.combiner(), messages);
        DoubleValue currValue = vertex.value();
        if (message.value() < currValue.value() || currValue.value() < 0.0) {
            vertex.value(message);
        }
        Id id = vertex.id();
        for (Edge edge :vertex.edges()) {
             if (edge.properties().get("inv") == null) {
                        continue;
             }
             DoubleValue weight = edge.property("weight");  
             if (weight == null) {
                 weight = new DoubleValue(1.0D);
             }
             currValue = vertex.value();  
             DoubleValue forwardvalue = new DoubleValue(weight.value() +
                                                   currValue.value()); 
             context.sendMessage(edge.targetId(), forwardvalue);
        }
    }
}
