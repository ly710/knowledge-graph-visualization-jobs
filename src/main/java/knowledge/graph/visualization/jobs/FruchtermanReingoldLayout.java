package knowledge.graph.visualization.jobs;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.Random;

public class FruchtermanReingoldLayout {
    private Integer temperature; //模拟退火火初始温度

    private Integer maxIter = 300; //算法迭代次数

    private Double c = 1d; // 节点距离控制系数

    private ExecutionEnvironment env;

    private String datasetName;

    private Integer rate;

    public FruchtermanReingoldLayout(
            ExecutionEnvironment env,
            String datasetName,
            int rate,
            double c
    ) {
        this.env = env;
        this.c = c;
        this.datasetName = datasetName;
        this.rate = rate;
    }

    public void run() throws Exception {
        DataSet<Tuple2<Long, String>> vertexes = env
                .readTextFile(Dir.BASE_DIR + datasetName + "/vertexes.tsv")
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String value) throws Exception {
                        String[] splits = value.split("\t");
                        return new Tuple2<>(Long.valueOf(splits[0]), splits[1]);
                    }
                });

        long vertexNum = vertexes.count();
        int length = ((int)Math.ceil((double)vertexNum / 1000) + 1) * 1000;

        DataSet<Tuple4<Long, String, Double, Double>> vertexesWithRandomPosition = vertexes
                .map(new SetRandomPositionMapFunction(length, length));


        DataSet<Tuple2<Long, Long>> edges = env
                .readTextFile(Dir.BASE_DIR + datasetName + "/graph.tsv")
                .map(new MapFunction<String, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(String value) throws Exception {
                        String[] splits = value.split("\t");
                        return new Tuple2<>(Long.valueOf(splits[0]), Long.valueOf(splits[2]));
                    }
                });

        DataSet<Tuple4<Long, String, Double, Double>> layout =
                layout(length, length, rate, c, maxIter, vertexesWithRandomPosition, edges)
                .map(new MapFunction<Tuple5<Long, String, Double, Double, Integer>, Tuple4<Long, String, Double, Double>>() {
                    @Override
                    public Tuple4<Long, String, Double, Double> map(Tuple5<Long, String, Double, Double, Integer> value) throws Exception {
                        return new Tuple4<>(value.f0, value.f1, value.f2, value.f3);
                    }
                });

        layout
                .map(new MapFunction<Tuple4<Long, String, Double, Double>, String>() {
                    @Override
                    public String map(Tuple4<Long, String, Double, Double> value) throws Exception {
                        return value.f0 + "\t" + value.f2 + "\t" + value.f3;
                    }
                })
                .writeAsText(Dir.BASE_DIR + datasetName + "/layout.vertexes.tsv")
                .setParallelism(1);

        env.execute("layout");
    }

    public static DataSet<Tuple5<Long, String, Double, Double, Integer>> layout(
            int w,
            int l,
            int rate,
            double c,
            int iterateTime,
            DataSet<Tuple4<Long, String, Double, Double>> vertexes,
            DataSet<Tuple2<Long, Long>> edges
    ) throws Exception {

        double k = c * Math.sqrt((w * l) / (double)vertexes.count());

        IterativeDataSet<Tuple5<Long, String, Double, Double, Integer>> iterativeDataSet = vertexes
                .map(new SetInitialTemperature(1))
                .iterate(iterateTime);

        DataSet<Tuple3<Long, Double, Double>> forceDisplacements = iterativeDataSet
                .cross(iterativeDataSet)
                .filter(new FilterFunction<Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>>>() {
                    @Override
                    public boolean filter(Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>> value) throws Exception {
                        return !value.f0.f0.equals(value.f1.f0);
                    }
                })
                .map(new ForceMapFunction(k));

        DataSet<Tuple3<Long, Double, Double>> attractiveDisplacements = edges
                .join(iterativeDataSet)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple5<Long, String, Double, Double, Integer>, Tuple2<Tuple5<Long, String, Double, Double, Integer>, Long>>() {
                    @Override
                    public Tuple2<Tuple5<Long, String, Double, Double, Integer>, Long> join(Tuple2<Long, Long> first, Tuple5<Long, String, Double, Double, Integer> second) throws Exception {
                        return new Tuple2<>(new Tuple5<>(second.f0, second.f1, second.f2, second.f3, second.f4), first.f1);
                    }
                })
                .join(iterativeDataSet)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Tuple5<Long, String, Double, Double, Integer>, Long>, Tuple5<Long, String, Double, Double, Integer>, Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>>>() {
                    @Override
                    public Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>> join(Tuple2<Tuple5<Long, String, Double, Double, Integer>, Long> first, Tuple5<Long, String, Double, Double, Integer> second) throws Exception {
                        return new Tuple2<>(new Tuple5<>(first.f0.f0, first.f0.f1, first.f0.f2, first.f0.f3, first.f0.f4), new Tuple5<>(second.f0, second.f1, second.f2, second.f3, second.f4));
                    }
                })
                .flatMap(new AttractiveMapFunction(k));

        DataSet<Tuple3<Long, Double, Double>> displacements = attractiveDisplacements
                .union(forceDisplacements)
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple3<Long, Double, Double>>() {
                    @Override
                    public Tuple3<Long, Double, Double> reduce(Tuple3<Long, Double, Double> value1, Tuple3<Long, Double, Double> value2) throws Exception {
                        return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<Long, Double, Double>, Tuple3<Long, Double, Double>>() {
                    @Override
                    public Tuple3<Long, Double, Double> map(Tuple3<Long, Double, Double> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f1, value.f2);
                    }
                });

        DataSet<Tuple5<Long, String, Double, Double, Integer>> newVertexes = iterativeDataSet
                .join(displacements)
                .where(0)
                .equalTo(0)
                .map(new SetDisplacementMapFunction(w, rate, iterateTime));

        return iterativeDataSet.closeWith(newVertexes);
    }

    public static class ForceMapFunction implements MapFunction<Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>>, Tuple3<Long, Double, Double>> {
        private final Double k;

        public ForceMapFunction(double k) {
            this.k = k;
        }

        @Override
        public Tuple3<Long, Double, Double> map(Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>> value) throws Exception {
            double deltaX = value.f0.f2 - value.f1.f2;
            double deltaY = value.f0.f3 - value.f1.f3;
            double deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
            double force = k * k / deltaLength;

            return new Tuple3<>(value.f0.f0, (deltaX / deltaLength) * force, (deltaY / deltaLength) * force);
        }
    }

    public static class AttractiveMapFunction implements FlatMapFunction<Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>>, Tuple3<Long, Double, Double>> {
        private final Double k;

        public AttractiveMapFunction(double k) {
            this.k = k;
        }

        @Override
        public void flatMap(Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Integer>> value, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
            double deltaX = value.f0.f2 - value.f1.f2;
            double deltaY = value.f0.f3 - value.f1.f3;
            double deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
            double force = deltaLength * deltaLength / k;

            double xDisplacement = (deltaX / deltaLength) * force;
            double yDisplacement = (deltaY / deltaLength) * force;

            out.collect(new Tuple3<>(value.f0.f0, -xDisplacement, -yDisplacement));
            out.collect(new Tuple3<>(value.f1.f0, xDisplacement, yDisplacement));
        }
    }

    public static class SetDisplacementMapFunction implements MapFunction<Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple3<Long, Double, Double>>, Tuple5<Long, String, Double, Double, Integer>> {
        private final Integer w;

        private final Integer rate;

        private final Integer maxIter;

        public SetDisplacementMapFunction(int w, int rate, int maxIter) {
            this.w = w;
            this.rate = rate;
            this.maxIter = maxIter;
        }

        @Override
        public Tuple5<Long, String, Double, Double, Integer> map(Tuple2<Tuple5<Long, String, Double, Double, Integer>, Tuple3<Long, Double, Double>> value) throws Exception {
            Random random = new Random();
            double randomX = 100 * random.nextDouble();
            double randomY = 100 * random.nextDouble();

            double dispLength = Math.sqrt(value.f1.f1 * value.f1.f1 + value.f1.f2 * value.f1.f2);

            double temperature = (double)w / rate;
            temperature = temperature * (1.0 - (double)value.f0.f4 / (double)maxIter);

            double x = Math.min(w - randomX, Math.max(0 + randomX, value.f0.f2 + (value.f1.f1 / dispLength * Math.min(dispLength, temperature))));
            double y = Math.min(w - randomY, Math.max(0 + randomY, value.f0.f3 + (value.f1.f2/ dispLength * Math.min(dispLength, temperature))));

            return new Tuple5<>(value.f0.f0, value.f0.f1, x, y, value.f0.f4 + 1);
        }
    }

    public static class SetRandomPositionMapFunction implements MapFunction<Tuple2<Long, String>, Tuple4<Long, String, Double, Double>> {
        private final Integer w;

        private final Integer l;

        public SetRandomPositionMapFunction(int w, int l) {
            this.w = w;
            this.l = l;
        }

        @Override
        public Tuple4<Long, String, Double, Double> map(Tuple2<Long, String> value) throws Exception {
            Random random = new Random();
            return new Tuple4<>(value.f0, value.f1, random.nextDouble() * w, random.nextDouble() * l);
        }
    }

    public static class SetInitialTemperature implements MapFunction<Tuple4<Long, String, Double, Double>, Tuple5<Long, String, Double, Double, Integer>> {
        private Integer iteratorNumber;

        public SetInitialTemperature(int iteratorNumber) {
            this.iteratorNumber = iteratorNumber;
        }

        @Override
        public Tuple5<Long, String, Double, Double, Integer> map(Tuple4<Long, String, Double, Double> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f2, value.f3, iteratorNumber);
        }
    }
}
