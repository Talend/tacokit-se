package org.talend.components.jdbc.containers;

import lombok.Data;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

@Data
public class DerbyTestContainer implements JdbcTestContainer {

    @Delegate
    private final DerbyContainer container = new DerbyContainer();

    @Override
    public String getDatabaseType() {
        return "Derby";
    }

    @Slf4j
    @Data
    public static class DerbyContainer {

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

        public boolean isRunning() {
            if (serverControl == null) {
                return false;
            }

            try {
                serverControl.ping();
                return true;
            } catch (final Exception e) {
                log.warn("Can't ping derby server. the database is starting...", e);
            }
            return false;
        }

        public String getJdbcUrl() {
            return "jdbc:derby://" + serverAdress.getHostAddress() + ":" + port + "/" + dbName;
        }

        public void start() {
            System.setProperty("DDL.stream.error.file", "target/derby.log");
            try {
                serverAdress = InetAddress.getByName("localhost");
                try (ServerSocket socket = new ServerSocket(0)) {
                    port = socket.getLocalPort();
                }
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
            try {
                serverControl = new NetworkServerControl(serverAdress, port);
                serverControl.start(new PrintWriter(System.out) {

                    @Override
                    public void close() {
                        super.flush();
                    }
                });
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }

            waitUntilStart();

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
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private void waitUntilStart() {
            int retry = 10;
            do {
                retry--;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } while (!isRunning() && retry > 0);

            if (!isRunning()) {
                throw new IllegalStateException("Can't start derby database, check the logs");
            }
        }

        public void stop() {
            if (serverControl != null) {
                try {
                    serverControl.shutdown();
                } catch (final Exception e) {
                    throw new IllegalStateException(e);
                }
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