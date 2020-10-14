package knowledge.graph.visualization.jobs;

        import org.apache.flink.api.java.ExecutionEnvironment;

public class JobReceiver {
    public static void main(String[] args) throws Exception {
        String jobName = args[0];

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String datasetName = args[1];

        switch (jobName) {
            case "to-graph":
                Tuples2Graph tuples2Graph = new Tuples2Graph(env, datasetName);
                tuples2Graph.sink();
                break;
            case "layout":
                Layout layout = new Layout(env, datasetName);
                layout.layout();
                break;
        }

        env.execute("job").getJobExecutionResult();
    }
}