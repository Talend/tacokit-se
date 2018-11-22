package org.talend.components.jdbc.derby;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.jdbc.BaseTest;
import org.talend.components.jdbc.JdbcContainer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public interface DerbyTestInstance extends JdbcContainer, BaseTest {

    String tableName = "test_" + UUID.randomUUID().toString().substring(0, 8);

    DerbyContainer container = new DerbyContainer();

    @BeforeAll
    default void start() throws Exception {
        container.start();
    }

    @AfterAll
    default void stop() throws Exception {
        container.close();
    }

    @BeforeEach
    default void beforeEach() {
        try (final Connection connection = getJdbcService().connection(newConnection())) {
            final String createTableQuery = "CREATE TABLE " + getTestTableName()
                    + " (id INT, t_string VARCHAR(30), t_boolean BOOLEAN, t_float FLOAT, t_double DOUBLE, t_bytes BLOB, t_date DATE, "
                    + "t_long BIGINT," + " PRIMARY KEY (id))";
            try (final PreparedStatement stm = connection.prepareStatement(createTableQuery)) {
                stm.execute();
            } catch (final SQLException e) {
                if (!"X0Y32".equals(e.getSQLState())) { // ignore if table already exist
                    throw e;
                }
                // truncate existing table
                try (final PreparedStatement stm = connection.prepareStatement("truncate table " + getTestTableName())) {
                    stm.execute();
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    default String getTestTableName() {
        return tableName;
    }

    default String getDatabaseType() {
        return "Derby";
    }

    default String getUsername() {
        return container.getUsername();
    }

    default String getPassword() {
        return container.getPassword();
    }

    default String getJdbcUrl() {
        return container.getJdbcURL();
    }

    @Slf4j
    @Data
    class DerbyContainer {

        private NetworkServerControl serverControl;

        private final String dbName;

        private final String username = "sa";

        private final String password = "sa";

        private InetAddress serverAdress;

        private int port;

        DerbyContainer() {
            try {
                this.dbName = Files.createTempDirectory("derby_" + UUID.randomUUID()).toFile().getAbsolutePath() + "/test";
            } catch (IOException e) {
                throw new IllegalStateException("can't create derby database file", e);
            }
        }

        private String getJdbcURL() {
            return "jdbc:derby://" + serverAdress.getHostAddress() + ":" + port + "/" + dbName;
        }

        void start() throws Exception {
            System.setProperty("DDL.stream.error.file", "target/derby.log");
            try {
                serverAdress = InetAddress.getByName("localhost");
                try (ServerSocket socket = new ServerSocket(0)) {
                    port = socket.getLocalPort();
                }
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
            serverControl = new NetworkServerControl(serverAdress, port);
            serverControl.start(new PrintWriter(System.out) {

                @Override
                public void close() {
                    super.flush();
                }
            });
            int tryCount = 10;
            while (tryCount > 0) {
                tryCount--;
                try {
                    serverControl.ping();
                    break;
                } catch (final Exception e) {
                    if (tryCount == 0) {
                        throw e;
                    }
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            final ClientDataSource dataSource = new ClientDataSource();
            dataSource.setCreateDatabase("create");
            dataSource.setDatabaseName(getDbName());
            dataSource.setServerName(serverAdress.getHostAddress());
            dataSource.setPortNumber(port);
            dataSource.setUser(username);
            dataSource.setPassword(password);
            try (Connection c = dataSource.getConnection()) { // validate server
                if (!c.isValid(3)) {
                    throw new IllegalStateException("Error while starting Derby server");
                }
                turnOnBuiltInUsers(c);
            }
        }

        void close() throws Exception {
            if (serverControl != null) {
                serverControl.shutdown();
            }
        }

        private void turnOnBuiltInUsers(final Connection conn) throws SQLException {
            try (Statement s = conn.createStatement()) {
                // Setting and Confirming requireAuthentication
                s.executeUpdate(
                        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.connection.requireAuthentication', 'true')");
                // Setting authentication scheme to Derby
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.authentication.provider', 'BUILTIN')");
                // Creating some sample users
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.sa', '" + "sa" + "')");
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.guest', '" + "sa" + "')");
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.admin', '" + "sa" + "')");
                // Setting default connection mode to no access
                s.executeUpdate(
                        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.defaultConnectionMode', 'noAccess')");

                // Defining read-write users
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.fullAccessUsers', 'sa,admin')");

                // Defining read-only users
                s.executeUpdate(
                        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.readOnlyAccessUsers', 'guest')");

                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.propertiesOnly', 'false')");
            }
        }

    }
}
