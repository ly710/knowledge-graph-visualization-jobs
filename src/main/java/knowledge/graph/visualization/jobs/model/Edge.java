package knowledge.graph.visualization.jobs.model;

import lombok.Data;

@Data
public class Edge {
    public Long sourceId;

    public Long endId;

    public Long predictId;

    public Edge(Long sourceId, Long endId, Long predictId) {
        this.sourceId = sourceId;
        this.endId = endId;
        this.predictId = predictId;
    }
}

