package org.apache.paimon.flink.action.cdc.postgresql;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashSet;
import java.util.Set;

public class PostgreSqlContainer extends JdbcDatabaseContainer {

    private static final String IMAGE_NAME = "postgres";
    private static final Integer POSTGRESQL_PORT = 5432;
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";

    private String databaseName = "test";
    private String username = "postgres";
    private String password = "password";

    public PostgreSqlContainer(PostgreSqlVersion postgreSqlVersion) {
        super(DockerImageName.parse(IMAGE_NAME + ":" + postgreSqlVersion.getVersion()));
        addExposedPort(POSTGRESQL_PORT);
    }
    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(getMappedPort(POSTGRESQL_PORT));
    }

    @Override
    protected void configure() {
        // Disable Postgres driver use of java.util.logging to reduce noise at startup time
        withUrlParam("loggerLevel", "OFF");
        addEnv("POSTGRES_DB", databaseName);
        addEnv("POSTGRES_USER", username);
        addEnv("POSTGRES_PASSWORD", password);
        setCommand("postgres -c config_file=/var/lib/postgresql/data/postgresql.conf");
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return (
                "jdbc:postgresql://" +
                        getHost() +
                        ":" +
                        getMappedPort(POSTGRESQL_PORT) +
                        "/" +
                        databaseName +
                        additionalUrlParams
        );
    }

    public int getDatabasePort() {
        return getMappedPort(POSTGRESQL_PORT);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    @SuppressWarnings("unchecked")
    public PostgreSqlContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }

    @Override
    public PostgreSqlContainer withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public PostgreSqlContainer withUsername(final String username) {
        this.username = username;
        return this;
    }

    @Override
    public PostgreSqlContainer withPassword(final String password) {
        this.password = password;
        return this;
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}
