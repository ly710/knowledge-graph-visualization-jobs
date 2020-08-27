package knowledge.graph.visualization.jobs;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.Random;

public class FruchtermanReingoldLayoutOrigin {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        int w = 5000;
        int l = 5000;

        DataSet<Tuple2<String, String>> taxonomy = env
                .readTextFile("input/yagoTaxonomy.tsv")
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] splits = value.split("\t");
                        return new Tuple2<>(splits[1], splits[3]);
                    }
                })
                .distinct();

        DataSet<Tuple1<String>> clazz = taxonomy
                .flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple1<String>>() {
                    @Override
                    public void flatMap(Tuple2<String, String> value, Collector<Tuple1<String>> out) throws Exception {
                        out.collect(new Tuple1<>(value.f0));
                        out.collect(new Tuple1<>(value.f1));
                    }
                })
                .distinct();

        DataSet<Tuple2<Long, String>> clazzWithUniqueId = DataSetUtils
                .zipWithIndex(clazz)
                .map(new MapFunction<Tuple2<Long, Tuple1<String>>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Tuple2<Long, Tuple1<String>> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1.f0);
                    }
                });

        DataSet<Tuple2<Long, Long>> edges = taxonomy
                .join(clazzWithUniqueId)
                .where(0)
                .equalTo(1)
                .with(new JoinFunction<Tuple2<String, String>, Tuple2<Long, String>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> join(Tuple2<String, String> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple2<>(second.f0, first.f1);
                    }
                })
                .join(clazzWithUniqueId)
                .where(1)
                .equalTo(1)
                .with(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> join(Tuple2<Long, String> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple2<>(first.f0, second.f0);
                    }
                });

        DataSet<Tuple4<Long, String, Double, Double>> vertexes = clazzWithUniqueId
                .map(new SetRandomPositionMapFunction(w, l));

        DataSet<Tuple4<Long, String, Double, Double>> finalVertexes = layout(w, l, 0.5, 1, vertexes, edges);

        finalVertexes
                .map(new MapFunction<Tuple4<Long, String, Double, Double>, String>() {
                    @Override
                    public String map(Tuple4<Long, String, Double, Double> value) throws Exception {
                        DecimalFormat decimalFormat = new DecimalFormat("#.##");
                        return value.f0 + "\t" + value.f1 + "\t" + decimalFormat.format(value.f2) + "\t" + decimalFormat.format(value.f3);
                    }
                })
                .writeAsText("output/classes.tsv")
                .setParallelism(1);

//        long beginTime = System.currentTimeMillis();
//        env.execute("layout");
//        long endTime = System.currentTimeMillis();
//        System.out.println(beginTime);
//        System.out.println(endTime);
//        System.out.println(endTime - beginTime);
        env.execute("a");
    }

    public static DataSet<Tuple4<Long, String, Double, Double>> layout(
            int w,
            int l,
            double c,
            int iterateTime,
            DataSet<Tuple4<Long, String, Double, Double>> vertexes,
            DataSet<Tuple2<Long, Long>> edges
    ) throws Exception {

        double k = c * Math.sqrt((w * l) / (double)vertexes.count());

        IterativeDataSet<Tuple4<Long, String, Double, Double>> iterativeDataSet = vertexes.iterate(iterateTime);

        DataSet<Tuple3<Long, Double, Double>> forceDisplacements = iterativeDataSet
                .cross(iterativeDataSet)
                .filter(new FilterFunction<Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>>>() {
                    @Override
                    public boolean filter(Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>> value) throws Exception {
                        return !value.f0.f0.equals(value.f1.f0);
                    }
                })
                .map(new ForceMapFunction(k));

        DataSet<Tuple3<Long, Double, Double>> attractiveDisplacements = edges
                .join(iterativeDataSet)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple4<Long, String, Double, Double>, Tuple2<Tuple4<Long, String, Double, Double>, Long>>() {
                    @Override
                    public Tuple2<Tuple4<Long, String, Double, Double>, Long> join(Tuple2<Long, Long> first, Tuple4<Long, String, Double, Double> second) throws Exception {
                        return new Tuple2<>(new Tuple4<>(second.f0, second.f1, second.f2, second.f3), first.f1);
                    }
                })
                .join(iterativeDataSet)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Tuple4<Long, String, Double, Double>, Long>, Tuple4<Long, String, Double, Double>, Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>>>() {
                    @Override
                    public Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>> join(Tuple2<Tuple4<Long, String, Double, Double>, Long> first, Tuple4<Long, String, Double, Double> second) throws Exception {
                        return new Tuple2<>(new Tuple4<>(first.f0.f0, first.f0.f1, first.f0.f2, first.f0.f3), new Tuple4<>(second.f0, second.f1, second.f2, second.f3));
                    }
                })
                .flatMap(new AttractiveMapFunction(k));

        DataSet<Tuple3<Long, Double, Double>> displacements = forceDisplacements
                .union(attractiveDisplacements)
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple3<Long, Double, Double>>() {
                    @Override
                    public Tuple3<Long, Double, Double> reduce(Tuple3<Long, Double, Double> value1, Tuple3<Long, Double, Double> value2) throws Exception {
                        return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                });

        DataSet<Tuple4<Long, String, Double, Double>> newVertexes = iterativeDataSet
                .join(displacements)
                .where(0)
                .equalTo(0)
                .map(new SetDisplacementMapFunction(w, l));

        return iterativeDataSet.closeWith(newVertexes);
    }

    public static class ForceMapFunction implements MapFunction<Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>>, Tuple3<Long, Double, Double>> {
        private final Double k;

        public ForceMapFunction(double k) {
            this.k = k;
        }

        @Override
        public Tuple3<Long, Double, Double> map(Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>> value) throws Exception {
            double deltaX = value.f1.f2 - value.f0.f2;
            double deltaY = value.f1.f3 - value.f0.f3;
            double deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
            double force = k * k / deltaLength;
            return new Tuple3<>(value.f0.f0, (deltaX / deltaLength) * force, (deltaY / deltaLength) * force);
        }
    }

    public static class AttractiveMapFunction implements FlatMapFunction<Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>>, Tuple3<Long, Double, Double>> {
        private final Double k;

        public AttractiveMapFunction(double k) {
            this.k = k;
        }

        @Override
        public void flatMap(Tuple2<Tuple4<Long, String, Double, Double>, Tuple4<Long, String, Double, Double>> value, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
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

    public static class SetDisplacementMapFunction implements MapFunction<Tuple2<Tuple4<Long, String, Double, Double>, Tuple3<Long, Double, Double>>, Tuple4<Long, String, Double, Double>> {
        private final Integer w;

        private final Integer l;

        public SetDisplacementMapFunction(int w, int l) {
            this.w = w;
            this.l = l;
        }

        @Override
        public Tuple4<Long, String, Double, Double> map(Tuple2<Tuple4<Long, String, Double, Double>, Tuple3<Long, Double, Double>> value) throws Exception {
            Random random = new Random();
            double randomX = 100 * random.nextDouble();
            double randomY = 100 * random.nextDouble();

            double x = Math.min(w - randomX, Math.max(0 + randomX, value.f0.f2 + value.f1.f1));
            double y = Math.min(l - randomY, Math.max(0 + randomY, value.f0.f3 + value.f1.f2));

            return new Tuple4<>(value.f0.f0, value.f0.f1, x, y);
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
}
