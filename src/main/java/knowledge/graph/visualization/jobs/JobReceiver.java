package knowledge.graph.visualization.jobs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;

import org.apache.flink.util.Collector;

public class JobReceiver {
    public static void main(String[] args) throws Exception {
        String datasetName = args[0];
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, String, String>> facts = env
                .readTextFile("/tmp/flink/input/" + datasetName + ".tsv")
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String value) throws Exception {
                        String[] splits = value.split("\t");

                        return new Tuple3<>(splits[1], splits[2], splits[3]);
                    }
                });

        DataSet<Tuple1<String>> entities = facts
                .flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple1<String>>() {
                    @Override
                    public void flatMap(Tuple3<String, String, String> value, Collector<Tuple1<String>> out) throws Exception {
                        out.collect(new Tuple1<>(value.f0));
                        out.collect(new Tuple1<>(value.f2));
                    }
                })
                .distinct();

        DataSet<Tuple1<String>> predicts = facts
                .map(new MapFunction<Tuple3<String, String, String>, Tuple1<String>>() {
                    @Override
                    public Tuple1<String> map(Tuple3<String, String, String> value) throws Exception {
                        return new Tuple1<>(value.f1);
                    }
                })
                .distinct();

        DataSet<Tuple2<Long, String>> entitiesWithId = DataSetUtils
                .zipWithUniqueId(entities)
                .map(new MapFunction<Tuple2<Long, Tuple1<String>>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Tuple1<String>> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1.f0);
                    }
                });

        DataSet<Tuple2<Long, String>> predictsWithId = DataSetUtils
                .zipWithUniqueId(predicts)
                .map(new MapFunction<Tuple2<Long, Tuple1<String>>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Tuple1<String>> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1.f0);
                    }
                });

        DataSet<Tuple3<Long, Long, Long>> idFacts = facts
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

        entitiesWithId
                .map(new MapFunction<Tuple2<Long, String>, String>() {
                    @Override
                    public String map(Tuple2<Long, String> value) throws Exception {
                        return value.f0 + "\t" + value.f1;
                    }
                })
                .writeAsText("/tmp/flink/output/" + datasetName + "-entities.tsv")
                .setParallelism(1);

        predictsWithId
                .map(new MapFunction<Tuple2<Long, String>, String>() {
                    @Override
                    public String map(Tuple2<Long, String> value) throws Exception {
                        return value.f0 + "\t" + value.f1;
                    }
                })
                .writeAsText("/tmp/flink/output/" + datasetName + "-predicts.tsv")
                .setParallelism(1);

        idFacts
                .map(new MapFunction<Tuple3<Long, Long, Long>, String>() {
                    @Override
                    public String map(Tuple3<Long, Long, Long> value) throws Exception {
                        return value.f0 + "\t" + value.f1 + "\t" + value.f2;
                    }
                })
                .writeAsText("/tmp/flink/output/" + datasetName + "-id-facts.tsv")
                .setParallelism(1);

        env.execute("pre execution");
    }
}


//    File fileOut = new File("/tmp/flink/test.log");
//    FileOutputStream fileOutputStream = new FileOutputStream(fileOut);
//    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
//        bufferedWriter.write("ttttttttttttttttt");
//                bufferedWriter.close();