package knowledge.graph.visualization.jobs.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class MysqlConfig implements Serializable
{
    private String connectionString;

    private String username;

    private String password;

    public MysqlConfig(String connectionString, String username, String password)
    {
        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
    }
}
