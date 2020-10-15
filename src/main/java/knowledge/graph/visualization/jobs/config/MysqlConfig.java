package knowledge.graph.visualization.jobs.config;

import lombok.Data;

@Data
public class MysqlConfig
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
