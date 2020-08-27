package knowledge.graph.visualization.jobs;

import org.apache.flink.api.java.ExecutionEnvironment;

public class Layout {
    private final String datasetName;

    private final ExecutionEnvironment env;

    public Layout(ExecutionEnvironment env, String datasetName) {
        this.env = env;
        this.datasetName = datasetName;
    }

    public void layout() throws Exception {
        FruchtermanReingoldLayout fruchtermanReingoldLayout = new FruchtermanReingoldLayout(
                env,
                datasetName,
                10,
                0.55
        );

        fruchtermanReingoldLayout.run();
    }
}
