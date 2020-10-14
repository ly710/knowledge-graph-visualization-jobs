package knowledge.graph.visualization.jobs;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Layout {
    private final String datasetName;

    private final ExecutionEnvironment env;

    public Layout(ExecutionEnvironment env, String datasetName) {
        this.env = env;
        this.datasetName = datasetName;
    }

    public void layout() throws Exception {
        DataSet<Tuple2<Long, String>> vertexes = env
                .readTextFile(Dir.BASE_DIR + datasetName + "/vertexes.tsv")
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String value) throws Exception {
                        String[] splits = value.split("\t");
                        return new Tuple2<>(Long.valueOf(splits[0]), splits[1]);
                    }
                });

        DataSet<Tuple2<Long, Long>> edges = env
                .readTextFile(Dir.BASE_DIR + datasetName + "/graph.tsv")
                .map(new MapFunction<String, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(String value) throws Exception {
                        String[] splits = value.split("\t");
                        return new Tuple2<>(Long.valueOf(splits[2]), Long.valueOf(splits[0]));
                    }
                })
                .distinct();

        Graph<Long, String, String> graph = new Tuples2FlinkGraph(
                env,
                vertexes,
                edges.map(new MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, String>>() {
                    @Override
                    public Tuple3<Long, Long, String> map(Tuple2<Long, Long> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f1, "");
                    }
                })
        ).getGraph();

        DataSet<Tuple2<Long, Double>> ranks = new PageRanker(graph,0.85, 100).rank();

        DataSet<Tuple3<Long, String, Double>> vertexesWithSize = vertexes
                .join(ranks)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, Double>, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> join(Tuple2<Long, String> first, Tuple2<Long, Double> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, 10 * Math.log(Math.E + second.f1 * 1000));
                    }
                });

        FruchtermanReingoldLayout fruchtermanReingoldLayout = new FruchtermanReingoldLayout(env, datasetName, 10, 0.3, vertexesWithSize, edges);

        fruchtermanReingoldLayout
            .run()
            .output(new MysqlSink("yago-sample", false))
            .setParallelism(1);

        DataSet<Tuple2<Long, Double>> minimapRanks = ranks
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .first(50);

        DataSet<Tuple2<Long, Long>> minimapEdges = edges
                .join(minimapRanks)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Double>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Double> second) throws Exception {
                        return first;
                    }
                })
                .join(minimapRanks)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Double>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Double> second) throws Exception {
                        return first;
                    }
                });

        DataSet<Tuple3<Long, String, Double>> minimapVertexesWithSize = minimapRanks
                .join(vertexes)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Double>, Tuple2<Long, String>, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> join(Tuple2<Long, Double> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple3<>(first.f0, second.f1, Math.log(Math.E + first.f1 * 1000));
                    }
                });

        FruchtermanReingoldLayout minimapFruchtermanReingoldLayout = new FruchtermanReingoldLayout(env, datasetName, 10, 0.3, minimapVertexesWithSize, minimapEdges, 2000);

        minimapFruchtermanReingoldLayout
                .run()
//                .collect();
                .output(new MysqlSink("yago-sample", true))
                .setParallelism(1);
    }

    public static class MysqlSink implements OutputFormat<Tuple2<Tuple5<Long, String, Double, Double, Double>, Tuple5<Long, String, Double, Double, Double>>>, Serializable {
        private final static String URL = "jdbc:mysql://52.83.174.95:7777/kg";
        public static final String USER = "dev";
        public static final String PASSWORD = "Dev^@dev2";

        private Connection connection;

        private String dataset;

        private Boolean minimap;

        private MysqlSink(String dataset, Boolean minimap) {
            this.dataset = dataset;
            this.minimap = minimap;
        }

        @Override
        public void configure(Configuration parameters) {
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection(URL, USER, PASSWORD);
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void writeRecord(Tuple2<Tuple5<Long, String, Double, Double, Double>, Tuple5<Long, String, Double, Double, Double>> record) throws IOException {
            try {
                String sql = "INSERT INTO `edge` (`dataset`, `minimap`, `source_id`, `target_id`, `source_label`, `target_label`, `source_size`, `target_size`, `edge_label`, `position`) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?))";
                //预编译
                PreparedStatement ptmt = connection.prepareStatement(sql); //预编译SQL，减少sql执行

                //传参
                ptmt.setString(1, dataset);
                ptmt.setInt(2, minimap ? 1 : 0);
                ptmt.setLong(3, record.f0.f0);
                ptmt.setLong(4, record.f1.f0);
                ptmt.setString(5, record.f0.f1);
                ptmt.setString(6, record.f1.f1);
                ptmt.setDouble(7, record.f0.f2.equals(Double.MIN_VALUE) ? 5.0 : record.f0.f2);
                ptmt.setDouble(8, record.f1.f2.equals(Double.MIN_VALUE) ? 5.0 : record.f1.f2);
                ptmt.setString(9, "");
                ptmt.setString(10, "LINESTRING(" + record.f0.f3 + " " + record.f0.f4 + "," + record.f1.f3 + " " + record.f1.f4 + ")");

                //执行
                ptmt.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
