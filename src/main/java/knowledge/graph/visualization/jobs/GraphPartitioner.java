package knowledge.graph.visualization.jobs;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class GraphPartitioner {
    public static void main(String[] args) throws Exception {
//    public void partition(String path) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, Double, Double>> nodesWithPosition = env
                .readTextFile("/tmp/flink/output/yago.layout.nodes.tsv")
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
                .readTextFile("/tmp/flink/output/yago-id-facts.tsv")
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
                .map(new MapFunction<Tuple5<Long, Integer, Integer, Double, Double>, String>() {
                    @Override
                    public String map(Tuple5<Long, Integer, Integer, Double, Double> value) throws Exception {
                        return value.f0 + "\t" + value.f1 + "\t" + value.f2 + "\t" + value.f3 + "\t" + value.f4;
                    }
                })
                .writeAsText("/tmp/flink/output/yago-nodes-block/yago-nodes-block.tsv")
                .setParallelism(1);

        edgesWithBlock
                .sortPartition(0, Order.ASCENDING)
                .setParallelism(1)
                .sortPartition(1, Order.ASCENDING)
                .setParallelism(1)
                .map(new MapFunction<Tuple4<Integer, Integer, Long, Long>, String>() {
                    @Override
                    public String map(Tuple4<Integer, Integer, Long, Long> value) throws Exception {
                        return value.f0 + "\t" + value.f1 + "\t" + value.f2 + "\t" + value.f3;
                    }
                })
                .writeAsText("/tmp/flink/output/yago-edges-block/yago-edges-block.tsv")
                .setParallelism(1);

        JobExecutionResult jobExecutionResult = env.execute("partition");
        jobExecutionResult.getNetRuntime();

        FileReader nodeFr = new FileReader("/tmp/flink/output/yago-nodes-block/yago-nodes-block.tsv");
        FileReader edgeFr = new FileReader("/tmp/flink/output/yago-edges-block/yago-edges-block.tsv");
        BufferedReader nodeBf = new BufferedReader(nodeFr);
        BufferedReader edgeBf = new BufferedReader(edgeFr);

        String str;
        String title = "";
        String newTitle;
        List<String> records = new ArrayList<>();
        // 按行读取字符串
        while ((str = nodeBf.readLine()) != null) {
            String[] split = str.split("\t");
            newTitle = split[1] + "-" + split[2];
            if(!title.equals(newTitle)) {
                if(records.size() > 0) {
                    File fileOut = new File("/tmp/flink/output/yago-nodes-block/" + title + ".tsv");

                    FileOutputStream fileOutputStream = new FileOutputStream(fileOut);
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

                    for (String r : records) {
                        bufferedWriter.write(r);
                        bufferedWriter.newLine();
                    }

                    bufferedWriter.close();
                    fileOutputStream.close();
                    records.clear();
                } else {
                    records.add(split[0] + "\t" + split[3] + "\t" + split[4]);
                }
                title = newTitle;
            } else {
                records.add(split[0] + "\t" + split[3] + "\t" + split[4]);
            }
        }

        while ((str = edgeBf.readLine()) != null) {
            String[] split = str.split("\t");
            newTitle = split[0] + "-" + split[1];
            if(!title.equals(newTitle)) {
                if(records.size() > 0) {
                    File fileOut = new File("/tmp/flink/output/yago-edges-block/" + title + ".tsv");

                    FileOutputStream fileOutputStream = new FileOutputStream(fileOut);
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

                    for (String r : records) {
                        bufferedWriter.write(r);
                        bufferedWriter.newLine();
                    }

                    bufferedWriter.close();
                    fileOutputStream.close();
                    records.clear();
                } else {
                    records.add(split[2] + "\t" + split[3]);
                }
                title = newTitle;
            } else {
                records.add(split[2] + "\t" + split[3]);
            }
        }

        nodeBf.close();
        nodeFr.close();
        edgeBf.close();
        edgeFr.close();
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

        public Integer getX() {
            return x;
        }

        public void setX(Integer x) {
            this.x = x;
        }

        public Integer getY() {
            return y;
        }

        public void setY(Integer y) {
            this.y = y;
        }
    }
}
