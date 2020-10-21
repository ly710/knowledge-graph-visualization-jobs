package knowledge.graph.visualization.jobs;

import knowledge.graph.visualization.jobs.config.MysqlConfig;
import knowledge.graph.visualization.jobs.job.CountPredicts;
import org.apache.flink.api.java.ExecutionEnvironment;

public class CountPredictsJob {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String dataset = args[0];
        String filePath = args[1];
        MysqlConfig mysqlConfig = new MysqlConfig(args[2], args[3], args[4]);

        CountPredicts countPredicts = new CountPredicts(env, dataset, filePath, mysqlConfig);
        countPredicts.run();
        env.execute("count predict");
    }
}
