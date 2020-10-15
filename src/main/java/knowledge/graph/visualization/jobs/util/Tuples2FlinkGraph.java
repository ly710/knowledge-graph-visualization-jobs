package knowledge.graph.visualization.jobs.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

public class Tuples2FlinkGraph {
    private final ExecutionEnvironment env;

    private DataSet<Tuple3<Long, Long, String>> edgeTuples;

    private DataSet<Tuple2<Long, String>> vertexTuples;

    public Tuples2FlinkGraph(
            ExecutionEnvironment env,
            DataSet<Tuple2<Long, String>> vertexTuples,
            DataSet<Tuple3<Long, Long, String>> edgeTuples
    ) {
        this.env = env;
        this.vertexTuples = vertexTuples;
        this.edgeTuples = edgeTuples;
    }

    public Graph<Long, String, String> getGraph() {
        return Graph.fromDataSet(getVertexes(), getEdges(), env);
    }

    public DataSet<Vertex<Long, String>> getVertexes() {
        return vertexTuples.map(new MapFunction<Tuple2<Long, String>, Vertex<Long, String>>() {
            @Override
            public Vertex<Long, String> map(Tuple2<Long, String> value) throws Exception {
                return new Vertex<>(value.f0, value.f1);
            }
        });
    }

    public DataSet<Edge<Long, String>> getEdges() {
        return edgeTuples.map(new MapFunction<Tuple3<Long, Long, String>, Edge<Long, String>>() {
            @Override
            public Edge<Long, String> map(Tuple3<Long, Long, String> value) throws Exception {
                return new Edge<>(value.f0, value.f1, value.f2);
            }
        });
    }
}
