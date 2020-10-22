package knowledge.graph.visualization.jobs.job;

import knowledge.graph.visualization.jobs.config.MysqlConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.Iterator;

public class CountPredicts {
    private final ExecutionEnvironment env;

    private final String tupleFilePath;

    private final MysqlConfig mysqlConfig;

    private final String dataset;

    public CountPredicts(
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
                .readTextFile(tupleFilePath)
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String value) throws Exception {
                        String[] splits = value.split("\t");

                        return new Tuple3<>(splits[0], splits[1], splits[2]);
                    }
                });


        DataSet<Tuple1<String>> predicts = tuples
                .map(new MapFunction<Tuple3<String, String, String>, Tuple1<String>>() {
                    @Override
                    public Tuple1<String> map(Tuple3<String, String, String> value) throws Exception {
                        return new Tuple1<>(value.f1);
                    }
                });

        predicts
                .partitionByHash(0)
                .groupBy(0)
                .combineGroup(new GroupCombineFunction<Tuple1<String>, Tuple2<String, Long>>() {
                    @Override
                    public void combine(Iterable<Tuple1<String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
                        Long count = 1L;
                        Iterator<Tuple1<String>> iterator = values.iterator();
                        String name = iterator.next().f0;
                        while(iterator.hasNext()) {
                            count++;
                            iterator.next();
                        }

                        out.collect(new Tuple2<>(name, count));
                    }
                })
                .partitionByHash(0)
                .output(new MysqlSink(dataset, mysqlConfig))
                .setParallelism(1);
    }

    public static class MysqlSink implements OutputFormat<Tuple2<String, Long>>, Serializable {
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
        public void writeRecord(Tuple2<String, Long> record) throws IOException {
            try {
                String sql = "INSERT INTO `predict` (`dataset`, `name`, `count`) VALUES ('" + dataset + "','" + record.f0 + "', " + record.f1 + ")";
                //预编译
                Statement statement = connection.createStatement();

                statement.executeUpdate(sql);
                statement.close();
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
