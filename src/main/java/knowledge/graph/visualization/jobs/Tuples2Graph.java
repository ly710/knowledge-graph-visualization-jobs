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

public class Tuples2Graph {
    private final ExecutionEnvironment env;

    private String datasetName;

    public Tuples2Graph(ExecutionEnvironment env, String datasetName) {
        this.env = env;
        this.datasetName = datasetName;
    }

    public void sink() {
        Tuple3<
                DataSet<Tuple2<Long, String>>,
                DataSet<Tuple2<Long, String>>,
                DataSet<Tuple3<Long, Long, Long>>
        > results = tuples2Graph();

        // id name
        results.f0
                .map(new MapFunction<Tuple2<Long, String>, String>() {
                    @Override
                    public String map(Tuple2<Long, String> value) throws Exception {
                        return value.f0 + "\t" + value.f1;
                    }
                })
                .writeAsText(Dir.BASE_DIR + datasetName + "/vertexes.tsv")
                .setParallelism(1);

        // id name
        results.f1
                .map(new MapFunction<Tuple2<Long, String>, String>() {
                    @Override
                    public String map(Tuple2<Long, String> value) throws Exception {
                        return value.f0 + "\t" + value.f1;
                    }
                })
                .writeAsText(Dir.BASE_DIR + datasetName + "/edges.tsv")
                .setParallelism(1);

        // vertex-id edge-id vertex-id
        results.f2
                .map(new MapFunction<Tuple3<Long, Long, Long>, String>() {
                    @Override
                    public String map(Tuple3<Long, Long, Long> value) throws Exception {
                        return value.f0 + "\t" + value.f1 + "\t" + value.f2;
                    }
                })
                .writeAsText(Dir.BASE_DIR + datasetName + "/graph.tsv")
                .setParallelism(1);
    }

    public Tuple3<
                DataSet<Tuple2<Long, String>>,
                DataSet<Tuple2<Long, String>>,
                DataSet<Tuple3<Long, Long, Long>>
            > tuples2Graph() {
        DataSet<Tuple3<String, String, String>> facts = env
                .readTextFile(Dir.BASE_DIR + datasetName + "/tuples" + ".tsv")
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String value) throws Exception {
                        String[] splits = value.split("\t");

                        return new Tuple3<>(splits[0], splits[1], splits[2]);
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

        return new Tuple3<>(entitiesWithId, predictsWithId, idFacts);
    }
}
