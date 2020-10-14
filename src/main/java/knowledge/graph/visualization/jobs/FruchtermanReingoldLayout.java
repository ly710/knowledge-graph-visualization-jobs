package knowledge.graph.visualization.jobs;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import java.util.Random;

public class FruchtermanReingoldLayout {
    private Integer temperature; //模拟退火火初始温度

    private Integer maxIter = 100; //算法迭代次数

    private Double c; // 节点距离控制系数

    private ExecutionEnvironment env;

    private String datasetName;

    private Integer rate;

    private DataSet<Tuple3<Long, String, Double>> vertexes;

    private DataSet<Tuple2<Long, Long>> edges;

    private Long length;

    public FruchtermanReingoldLayout(
            ExecutionEnvironment env,
            String datasetName,
            int rate,
            double c,
            DataSet<Tuple3<Long, String, Double>> vertexes,
            DataSet<Tuple2<Long, Long>> edges
    ) {
        this.env = env;
        this.c = c;
        this.datasetName = datasetName;
        this.rate = rate;
        this.vertexes = vertexes;
        this.edges = edges;
    }

    public FruchtermanReingoldLayout(
            ExecutionEnvironment env,
            String datasetName,
            int rate,
            double c,
            DataSet<Tuple3<Long, String, Double>> vertexes,
            DataSet<Tuple2<Long, Long>> edges,
            long length
    ) {
        this.env = env;
        this.c = c;
        this.datasetName = datasetName;
        this.rate = rate;
        this.vertexes = vertexes;
        this.edges = edges;
        this.length = length;
    }

    public DataSet<Tuple2<Tuple5<Long, String, Double, Double, Double>, Tuple5<Long, String, Double, Double, Double>>> run() throws Exception {
        long vertexNum = vertexes.count();
        if(length == null) {
            length = ((long) Math.ceil((double) vertexNum / 1000) + 1) * 1000 * 2;
        }

        DataSet<Tuple5<Long, String, Double, Double, Double>> vertexesWithRandomPosition = vertexes
                .map(new SetRandomPositionMapFunction(length, length));

        DataSet<Tuple5<Long, String, Double, Double, Double>> layout =
                layout(length, length, rate, c, maxIter, vertexesWithRandomPosition)
                .map(new MapFunction<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple5<Long, String, Double, Double, Double>>() {
                    @Override
                    public Tuple5<Long, String, Double, Double, Double> map(Tuple6<Long, String, Double, Double, Double, Integer> value) throws Exception {
                        return new Tuple5<>(value.f0, value.f1, value.f2, value.f3, value.f4);
                    }
                });

        return edges
                .join(layout)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple5<Long, String, Double, Double, Double>, Tuple2<Tuple5<Long, String, Double, Double, Double>, Long>>() {
                    @Override
                    public Tuple2<Tuple5<Long, String, Double, Double, Double>, Long> join(Tuple2<Long, Long> first, Tuple5<Long, String, Double, Double, Double> second) throws Exception {
                        return new Tuple2<>(second, first.f1);
                    }
                })
                .join(layout)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Tuple5<Long, String, Double, Double, Double>, Long>, Tuple5<Long, String, Double, Double, Double>, Tuple2<Tuple5<Long, String, Double, Double, Double>, Tuple5<Long, String, Double, Double, Double>>>() {
                    @Override
                    public Tuple2<Tuple5<Long, String, Double, Double, Double>, Tuple5<Long, String, Double, Double, Double>> join(Tuple2<Tuple5<Long, String, Double, Double, Double>, Long> first, Tuple5<Long, String, Double, Double, Double> second) throws Exception {
                        return new Tuple2<>(first.f0, second);
                    }
                });
    }

    public DataSet<Tuple6<Long, String, Double, Double, Double, Integer>> layout(
            long w,
            long l,
            int rate,
            double c,
            int iterateTime,
            DataSet<Tuple5<Long, String, Double, Double, Double>> vertexesWithPosition
    ) throws Exception {

        double k = c * Math.sqrt((w * l) / (double)vertexes.count());

        IterativeDataSet<Tuple6<Long, String, Double, Double, Double, Integer>> iterativeDataSet = vertexesWithPosition
                .map(new SetInitialTemperature(1))
                .iterate(iterateTime);

        DataSet<Tuple3<Long, Double, Double>> forceDisplacements = iterativeDataSet
                .cross(iterativeDataSet)
                .filter(new FilterFunction<Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>>>() {
                    @Override
                    public boolean filter(Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>> value) throws Exception {
                        return !value.f0.f0.equals(value.f1.f0);
                    }
                })
                .map(new ForceMapFunction(k));

        DataSet<Tuple3<Long, Double, Double>> attractiveDisplacements = edges
                .join(iterativeDataSet)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple6<Long, String, Double, Double, Double, Integer>, Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Long>>() {
                    @Override
                    public Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Long> join(Tuple2<Long, Long> first, Tuple6<Long, String, Double, Double, Double, Integer> second) throws Exception {
                        return new Tuple2<>(second, first.f1);
                    }
                })
                .join(iterativeDataSet)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Long>, Tuple6<Long, String, Double, Double, Double, Integer>, Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>>>() {
                    @Override
                    public Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>> join(Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Long> first, Tuple6<Long, String, Double, Double, Double, Integer> second) throws Exception {
                        return new Tuple2<>(first.f0, second);
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

        DataSet<Tuple6<Long, String, Double, Double, Double, Integer>> newVertexes = iterativeDataSet
                .join(displacements)
                .where(0)
                .equalTo(0)
                .map(new SetDisplacementMapFunction(w, rate, iterateTime));

        return iterativeDataSet.closeWith(newVertexes);
    }

    public static class ForceMapFunction implements MapFunction<Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>>, Tuple3<Long, Double, Double>> {
        private final Double k;

        public ForceMapFunction(double k) {
            this.k = k;
        }

        @Override
        public Tuple3<Long, Double, Double> map(Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>> value) throws Exception {
            double deltaX = value.f0.f3 - value.f1.f3;
            double deltaY = value.f0.f4 - value.f1.f4;
            double deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY) - (value.f0.f2 + value.f1.f2);
            double force = k * k / deltaLength;

            return new Tuple3<>(value.f0.f0, (deltaX / deltaLength) * force, (deltaY / deltaLength) * force);
        }
    }

    public static class AttractiveMapFunction implements FlatMapFunction<Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>>, Tuple3<Long, Double, Double>> {
        private final Double k;

        public AttractiveMapFunction(double k) {
            this.k = k;
        }

        @Override
        public void flatMap(Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple6<Long, String, Double, Double, Double, Integer>> value, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
            double deltaX = value.f0.f3 - value.f1.f3;
            double deltaY = value.f0.f4 - value.f1.f4;
            double deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY) - (value.f0.f2 + value.f1.f2);
            double force = deltaLength * deltaLength / k;

            double xDisplacement = (deltaX / deltaLength) * force;
            double yDisplacement = (deltaY / deltaLength) * force;

            out.collect(new Tuple3<>(value.f0.f0, -xDisplacement, -yDisplacement));
            out.collect(new Tuple3<>(value.f1.f0, xDisplacement, yDisplacement));
        }
    }

    public static class SetDisplacementMapFunction implements MapFunction<Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple3<Long, Double, Double>>, Tuple6<Long, String, Double, Double, Double, Integer>> {
        private final Long w;

        private final Integer rate;

        private final Integer maxIter;

        public SetDisplacementMapFunction(long w, int rate, int maxIter) {
            this.w = w;
            this.rate = rate;
            this.maxIter = maxIter;
        }

        @Override
        public Tuple6<Long, String, Double, Double, Double, Integer> map(Tuple2<Tuple6<Long, String, Double, Double, Double, Integer>, Tuple3<Long, Double, Double>> value) throws Exception {
            Random random = new Random();
            double randomX = 100 * random.nextDouble();
            double randomY = 100 * random.nextDouble();

            double dispLength = Math.sqrt(value.f1.f1 * value.f1.f1 + value.f1.f2 * value.f1.f2);

            double temperature = (double)w / rate;
            temperature = temperature * (1.0 - value.f0.f5 / (double)maxIter);

            double x = Math.min(w - randomX, Math.max(0 + randomX, value.f0.f3 + (value.f1.f1 / dispLength * Math.min(dispLength, temperature))));
            double y = Math.min(w - randomY, Math.max(0 + randomY, value.f0.f4 + (value.f1.f2/ dispLength * Math.min(dispLength, temperature))));

            return new Tuple6<>(value.f0.f0, value.f0.f1, value.f0.f2,  x, y, value.f0.f5 + 1);
        }
    }

    public static class SetRandomPositionMapFunction implements MapFunction<Tuple3<Long, String, Double>, Tuple5<Long, String, Double, Double, Double>> {
        private final Long w;

        private final Long l;

        public SetRandomPositionMapFunction(Long w, Long l) {
            this.w = w;
            this.l = l;
        }

        @Override
        public Tuple5<Long, String, Double, Double, Double> map(Tuple3<Long, String, Double> value) throws Exception {
            Random random = new Random();
            return new Tuple5<>(value.f0, value.f1, value.f2, random.nextDouble() * w, random.nextDouble() * l);
        }
    }

    public static class SetInitialTemperature implements MapFunction<Tuple5<Long, String, Double, Double, Double>, Tuple6<Long, String, Double, Double, Double, Integer>> {
        private Integer iteratorNumber;

        public SetInitialTemperature(int iteratorNumber) {
            this.iteratorNumber = iteratorNumber;
        }

        @Override
        public Tuple6<Long, String, Double, Double, Double, Integer> map(Tuple5<Long, String, Double, Double, Double> value) throws Exception {
            return new Tuple6<>(value.f0, value.f1, value.f2, value.f3, value.f4, iteratorNumber);
        }
    }
}
