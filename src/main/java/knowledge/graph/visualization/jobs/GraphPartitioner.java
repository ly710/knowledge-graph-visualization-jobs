package knowledge.graph.visualization.jobs;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class GraphPartitioner {
    private final ExecutionEnvironment env;

    private final String datasetName;

    public GraphPartitioner(ExecutionEnvironment env, String datasetName) {
        this.env = env;
        this.datasetName = datasetName;
    }

    public void partition() throws Exception {
        DataSet<Tuple3<Long, Double, Double>> nodesWithPosition = env
                .readTextFile(Dir.BASE_DIR + datasetName + "/layout.vertexes.tsv")
                .map(new MapFunction<String, Tuple3<Long, Double, Double>>() {
                    @Override
                    public Tuple3<Long, Double, Double> map(String value) throws Exception {
                        String[] splits = value.split("\t");
                        return new Tuple3<>(Long.valueOf(splits[0]), Double.valueOf(splits[1]), Double.valueOf(splits[2]));
                    }
                });

        DataSet<Tuple5<Long, Integer, Integer, Double, Double>> nodesWithBlock = nodesWithPosition
                .map(new MapFunction<Tuple3<Long, Double, Double>, Tuple5<Long, Integer, Integer, Double, Double>>() {
                    @Override
                    public Tuple5<Long, Integer, Integer, Double, Double> map(Tuple3<Long, Double, Double> value) throws Exception {
                        return new Tuple5<>(
                                value.f0,
                                (int)(value.f1 / 500),
                                (int)(value.f2 / 500),
                                value.f1,
                                value.f2
                        );
                    }
                });

        DataSet<Tuple2<Long, Long>> edges = env
                .readTextFile(Dir.BASE_DIR + datasetName + "/graph.tsv")
                .map(new MapFunction<String, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(String value) throws Exception {
                        String[] splits = value.split("\t");
                        return new Tuple2<>(Long.valueOf(splits[0]), Long.valueOf(splits[2]));
                    }
                });

        DataSet<Tuple4<Integer, Integer, Long, Long>> edgesWithBlock = edges
                .join(nodesWithBlock)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple5<Long, Integer, Integer, Double, Double>, Tuple2<Tuple3<Integer, Integer, Long>, Long>>() {
                    @Override
                    public Tuple2<Tuple3<Integer, Integer, Long>, Long> join(Tuple2<Long, Long> first, Tuple5<Long, Integer, Integer, Double, Double> second) throws Exception {
                        return new Tuple2<>(new Tuple3<>(second.f1, second.f2, first.f0), first.f1);
                    }
                })
                .join(nodesWithBlock)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Tuple3<Integer, Integer, Long>, Long>, Tuple5<Long, Integer, Integer, Double, Double>, Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>>() {
                    @Override
                    public Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> join(Tuple2<Tuple3<Integer, Integer, Long>, Long> first, Tuple5<Long, Integer, Integer, Double, Double> second) throws Exception {
                        return new Tuple2<>(first.f0, new Tuple3<>(second.f1, second.f2, first.f1));
                    }
                })
                .flatMap(new FlatMapFunction<Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>, Tuple4<Integer, Integer, Long, Long>>() {
                    @Override
                    public void flatMap(Tuple2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> value, Collector<Tuple4<Integer, Integer, Long, Long>> out) throws Exception {
                        out.collect(new Tuple4<>(value.f0.f0, value.f0.f1, value.f0.f2, value.f1.f2));
                        out.collect(new Tuple4<>(value.f1.f0, value.f1.f1, value.f1.f2, value.f0.f2));
                    }
                })
                .distinct();

        nodesWithBlock
                .sortPartition(1, Order.ASCENDING)
                .setParallelism(1)
                .sortPartition(2, Order.ASCENDING)
                .setParallelism(1)
                .output(new VertexPartitionSink(Dir.BASE_DIR + datasetName + "/vertexes-block/"))
                .setParallelism(1);

        edgesWithBlock
                .sortPartition(0, Order.ASCENDING)
                .setParallelism(1)
                .sortPartition(1, Order.ASCENDING)
                .setParallelism(1)
                .output(new EdgePartitionSink(Dir.BASE_DIR + datasetName + "/edges-block/"))
                .setParallelism(1);

        env.execute("partition");
    }

    public static Block getBlock(double x, double y) {
        int blockX = (int)(x / 500);
        int blockY = (int)(y / 500);

        return new Block(blockX, blockY);
    }

    public static int range(int from, int to) {
        return to - from;
    }

    public static class Block {
        public Integer x;

        public Integer y;

        public Block(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    public static class VertexPartitionSink implements OutputFormat<Tuple5<Long, Integer, Integer, Double, Double>> {
        private String path;

        private FileOutputStream fileWriter;

        private BufferedWriter bufferedWriter;

        private String currentPartition;

        private List<Tuple5<Long, Integer, Integer, Double, Double>> currentPartitionRecords = new ArrayList<>();

        public VertexPartitionSink(String path) {
            this.path = path;
        }

        @Override
        public void configure(Configuration parameters) {
            //.
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            //..
        }

        @Override
        public void writeRecord(Tuple5<Long, Integer, Integer, Double, Double> record) throws IOException {
            String partition = record.f1 + "-" + record.f2;

            if(fileWriter == null) {
                fileWriter = new FileOutputStream(path + partition + ".tsv");
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileWriter));
                currentPartitionRecords.add(record);
                currentPartition = partition;
                return;
            }

            if(!partition.equals(currentPartition)) {
                for (Tuple5<Long, Integer, Integer, Double, Double> r : currentPartitionRecords) {
                    bufferedWriter.write(r.f0 + "\t" + r.f3 + "\t" + r.f4);
                    bufferedWriter.newLine();
                }

                bufferedWriter.close();
                fileWriter.close();

                currentPartitionRecords.clear();

                currentPartition = partition;
                fileWriter = new FileOutputStream(path + currentPartition + ".tsv");
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileWriter));
            }

            currentPartitionRecords.add(record);
        }

        @Override
        public void close() throws IOException {
            for (Tuple5<Long, Integer, Integer, Double, Double> r : currentPartitionRecords) {
                bufferedWriter.write(r.f0 + "\t" + r.f3 + "\t" + r.f4);
                bufferedWriter.newLine();
            }

            bufferedWriter.close();
            fileWriter.close();
        }
    }

    public static class EdgePartitionSink implements OutputFormat<Tuple4<Integer, Integer, Long, Long>> {
        private String path;

        private FileOutputStream fileWriter;

        private BufferedWriter bufferedWriter;

        private String currentPartition;

        private List<Tuple4<Integer, Integer, Long, Long>> currentPartitionRecords = new ArrayList<>();

        public EdgePartitionSink(String path) {
            this.path = path;
        }

        @Override
        public void configure(Configuration parameters) {
            //.
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            //..
        }

        @Override
        public void writeRecord(Tuple4<Integer, Integer, Long, Long> record) throws IOException {
            String partition = record.f0 + "-" + record.f1;

            if(fileWriter == null) {
                fileWriter = new FileOutputStream(path + partition + ".tsv");
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileWriter));
                currentPartitionRecords.add(record);
                currentPartition = partition;
                return;
            }

            if(!partition.equals(currentPartition)) {
                for (Tuple4<Integer, Integer, Long, Long> r : currentPartitionRecords) {
                    bufferedWriter.write(r.f2 + "\t" + r.f3);
                    bufferedWriter.newLine();
                }

                bufferedWriter.close();
                fileWriter.close();

                currentPartitionRecords.clear();

                currentPartition = partition;
                fileWriter = new FileOutputStream(path + currentPartition + ".tsv");
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileWriter));
            }

            currentPartitionRecords.add(record);
        }

        @Override
        public void close() throws IOException {
            for (Tuple4<Integer, Integer, Long, Long> r : currentPartitionRecords) {
                bufferedWriter.write(r.f2 + "\t" + r.f3);
                bufferedWriter.newLine();
            }

            bufferedWriter.close();
            fileWriter.close();
        }
    }
}
