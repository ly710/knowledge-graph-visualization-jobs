package knowledge.graph.visualization.jobs.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.linkanalysis.PageRank;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PageRanker {
    public Graph<Long, String, String> graph;

    private Double dampingFactor;

    private Integer iterations;

    public PageRanker(Graph<Long, String, String> graph, double dampingFactor, int iterations) {
        this.graph = graph;
        this.dampingFactor = dampingFactor;
        this.iterations = iterations;
    }

    public DataSet<Tuple2<Long, Double>> rank() throws Exception {
        return graph
                .run(new PageRank<>(dampingFactor, iterations))
                .map(new MapFunction<PageRank.Result<Long>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(PageRank.Result<Long> value) throws Exception {
                        return new Tuple2<>(value.getVertexId0(), value.getPageRankScore().getValue());
                    }
                });
    }
}
