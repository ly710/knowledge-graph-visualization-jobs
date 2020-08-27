package knowledge.graph.visualization.util;

public class Edge {
    public Long sourceId;

    public Long endId;

    public Long predictId;

    public Edge(Long sourceId, Long endId, Long predictId) {
        this.sourceId = sourceId;
        this.endId = endId;
        this.predictId = predictId;
    }

    public Long getSourceId() {
        return sourceId;
    }

    public void setSourceId(Long sourceId) {
        this.sourceId = sourceId;
    }

    public Long getEndId() {
        return endId;
    }

    public void setEndId(Long endId) {
        this.endId = endId;
    }

    public Long getPredictId() {
        return predictId;
    }

    public void setPredictId(Long predictId) {
        this.predictId = predictId;
    }
}

