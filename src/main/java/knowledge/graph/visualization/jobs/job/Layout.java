package knowledge.graph.visualization.jobs.job;

import knowledge.graph.visualization.jobs.config.MysqlConfig;
import knowledge.graph.visualization.jobs.util.PageRanker;
import knowledge.graph.visualization.jobs.util.FruchtermanReingoldLayout;
import knowledge.graph.visualization.jobs.util.Tuples2FlinkGraph;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Layout {
    private final ExecutionEnvironment env;

    private final String tupleFilePath;

    private final MysqlConfig mysqlConfig;

    private final String dataset;

    public Layout(
            ExecutionEnvironment env,
            String dataset,
            String tupleFilePath,
            MysqlConfig mysqlConfig
    ) {
        this.env = env;
        this.dataset = dataset;
        this.tupleFilePath = tupleFilePath;
        this.mysqlConfig = mysqlConfig;
    }

    public void run() throws Exception {
        DataSet<Tuple3<String, String, String>> tuples = env
                .readTextFile(tupleFilePath + "/" + dataset + ".tuples.tsv")
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String value) throws Exception {
                        String[] splits = value.split("\t");

                        return new Tuple3<>(splits[0], splits[1], splits[2]);
                    }
                });

        DataSet<Tuple1<String>> entities = tuples
                .flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple1<String>>() {
                    @Override
                    public void flatMap(Tuple3<String, String, String> value, Collector<Tuple1<String>> out) throws Exception {
                        out.collect(new Tuple1<>(value.f0));
                        out.collect(new Tuple1<>(value.f2));
                    }
                })
                .distinct();

        DataSet<Tuple1<String>> predicts = tuples
                .map(new MapFunction<Tuple3<String, String, String>, Tuple1<String>>() {
                    @Override
                    public Tuple1<String> map(Tuple3<String, String, String> value) throws Exception {
                        return new Tuple1<>(value.f1);
                    }
                })
                .distinct();

        //(id, name) entity
        DataSet<Tuple2<Long, String>> entitiesWithId = DataSetUtils
                .zipWithUniqueId(entities)
                .map(new MapFunction<Tuple2<Long, Tuple1<String>>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Tuple1<String>> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1.f0);
                    }
                });

        //(id, name) predict
        DataSet<Tuple2<Long, String>> predictsWithId = DataSetUtils
                .zipWithUniqueId(predicts)
                .map(new MapFunction<Tuple2<Long, Tuple1<String>>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Tuple1<String>> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1.f0);
                    }
                });

        //(entityId, predictId, entityId)
        DataSet<Tuple3<Long, Long, Long>> idTuples = tuples
                .join(entitiesWithId)
                .where(0)
                .equalTo(1)
                .with(new JoinFunction<Tuple3<String, String, String>, Tuple2<Long, String>, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> join(Tuple3<String, String, String> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple3<>(second.f0, first.f1, first.f2);
                    }
                })
                .join(entitiesWithId)
                .where(2)
                .equalTo(1)
                .with(new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, String>, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> join(Tuple3<Long, String, String> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f0);
                    }
                })
                .join(predictsWithId)
                .where(1)
                .equalTo(1)
                .with(new JoinFunction<Tuple3<Long, String, Long>, Tuple2<Long, String>, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> join(Tuple3<Long, String, Long> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple3<>(first.f0, second.f0, first.f2);
                    }
                });

        Graph<Long, String, String> graph = new Tuples2FlinkGraph(
                env,
                entitiesWithId,
                idTuples.map(new MapFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, String>>() {
                            @Override
                            public Tuple3<Long, Long, String> map(Tuple3<Long, Long, Long> value) throws Exception {
                                return new Tuple3<>(value.f0, value.f2, "");
                            }
                        })
        )
                .getGraph();

        DataSet<Tuple2<Long, Double>> ranks = new PageRanker(graph,0.85, 100).rank();

        DataSet<Tuple3<Long, String, Double>> vertexesWithSize = entitiesWithId
                .join(ranks)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, Double>, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> join(Tuple2<Long, String> first, Tuple2<Long, Double> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, 10 * Math.log(Math.E + second.f1 * 1000));
                    }
                });

        FruchtermanReingoldLayout fruchtermanReingoldLayout = new FruchtermanReingoldLayout(
                10,
                0.3,
                vertexesWithSize,
                idTuples.map(new MapFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Tuple3<Long, Long, Long> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f2);
                    }
                }),
                mysqlConfig,
                dataset
        );

        fruchtermanReingoldLayout
                .run()
                .output(new MysqlSink(dataset, mysqlConfig))
                .setParallelism(1);
    }

    public static class MysqlSink implements OutputFormat<Tuple2<Tuple5<Long, String, Double, Double, Double>, Tuple5<Long, String, Double, Double, Double>>>, Serializable {
        final Logger log = LoggerFactory.getLogger(MysqlSink.class);

        private Connection connection;

        private final MysqlConfig mysqlConfig;

        private final String dataset;

        private MysqlSink(String dataset, MysqlConfig mysqlConfig) {
            this.dataset = dataset;
            this.mysqlConfig = mysqlConfig;
        }

        @Override
        public void configure(Configuration parameters) {
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection(mysqlConfig.getConnectionString(), mysqlConfig.getUsername(), mysqlConfig.getPassword());
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void writeRecord(Tuple2<Tuple5<Long, String, Double, Double, Double>, Tuple5<Long, String, Double, Double, Double>> record) throws IOException {
            try {
                String sql = "INSERT INTO `edge` (`dataset`, `source_id`, `target_id`, `source_label`, `target_label`, `source_size`, `target_size`, `edge_label`, `position`) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?))";
                //预编译
                PreparedStatement ptmt = connection.prepareStatement(sql); //预编译SQL，减少sql执行

                //传参
                ptmt.setString(1, dataset);
                ptmt.setLong(2, record.f0.f0);
                ptmt.setLong(3, record.f1.f0);
                ptmt.setString(4, record.f0.f1);
                ptmt.setString(5, record.f1.f1);
                ptmt.setDouble(6, record.f0.f2.equals(Double.MIN_VALUE) ? 5.0 : record.f0.f2);
                ptmt.setDouble(7, record.f1.f2.equals(Double.MIN_VALUE) ? 5.0 : record.f1.f2);
                ptmt.setString(8, "");
                ptmt.setString(9, "LINESTRING(" + record.f0.f3 + " " + record.f0.f4 + "," + record.f1.f3 + " " + record.f1.f4 + ")");

                //执行
                ptmt.execute();
            } catch (SQLException e) {
                log.error("mysql error", e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("mysql sql error when close connection", e);
            }
        }
    }
}
