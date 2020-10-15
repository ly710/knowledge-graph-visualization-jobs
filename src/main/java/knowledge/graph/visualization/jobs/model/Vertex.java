package knowledge.graph.visualization.jobs.model;

import lombok.Data;

@Data
public class Vertex {
    public Long id;

    public Double x;

    public Double y;

    public String name;

    public Integer size = 12;

    public Vertex(Long id, String name, Double x, Double y) {
        this.id = id;
        this.name = name;
        this.x = x;
        this.y = y;
    }
}
