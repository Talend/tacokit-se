/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DerbyExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private NetworkServerControl serverControl;

    private ClientDataSource dataSource;

    private DerbyInfo derbyDb;

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        final WithDerby element = context.getElement().map(e -> e.getAnnotation(WithDerby.class))
                .orElseThrow(() -> new IllegalArgumentException("No annotation @WithDerby on " + context.getRequiredTestClass()));

        final int port;
        if (element.port() == 0) {
            ServerSocket socket = new ServerSocket(0);
            port = socket.getLocalPort();
            socket.close();
        } else {
            port = element.port();
        }

        if (port == 0) {
            throw new IllegalStateException("Can't execute a free port for derby database");
        }

        System.setProperty("derby.stream.error.file", element.logFile());
        final String dbName = Files.createTempDirectory("derby").toFile().getAbsolutePath() + "/" + element.dbName();
        final String url = "jdbc:derby://" + element.server() + ":" + port + "/" + dbName;
        final InetAddress serverAdress = InetAddress.getByName(element.server());
        serverControl = new NetworkServerControl(serverAdress, port);
        serverControl.start(new PrintWriter(System.out) {

            @Override
            public void close() {
                super.flush();
            }
        });
        int tryCount = 5; // wait for server to start
        while (tryCount > 0) {
            tryCount--;
            try {
                serverControl.ping();
                break;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        dataSource = new ClientDataSource();
        if (element.createDb()) {
            dataSource.setCreateDatabase("create");
        }
        dataSource.setDatabaseName(dbName);
        dataSource.setServerName(serverAdress.getHostAddress());
        dataSource.setPortNumber(port);
        dataSource.setUser(element.user());
        if (!element.password().isEmpty()) {
            dataSource.setPassword(element.password());
        }
        try (Connection c = dataSource.getConnection()) { // validate server
            if (!c.isValid(3)) {
                throw new IllegalStateException("Error while starting Derby server");
            }
            turnOnBuiltInUsers(c, element.password());

            if (!element.onStartSQLScript().isEmpty()) {
                execScript(element.onStartSQLScript(), c);
            }
        }

        derbyDb = new DerbyInfo(serverAdress.getHostAddress(), port, dbName);
    }

    private void execScript(final String script, final Connection c) {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(script)) {
            new BufferedReader(new InputStreamReader(is)).lines().filter(l -> !l.isEmpty()).forEach(query -> {
                try (Statement statement = c.createStatement()) {
                    statement.executeUpdate(query);
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void afterAll(final ExtensionContext context) throws Exception {
        final WithDerby element = context.getElement().map(e -> e.getAnnotation(WithDerby.class))
                .orElseThrow(() -> new IllegalArgumentException("No annotation @WithDerby on " + context.getRequiredTestClass()));
        try {
            if (!element.onShutdownSQLScript().isEmpty()) {
                try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(element.onShutdownSQLScript())) {
                    new BufferedReader(new InputStreamReader(is)).lines().filter(l -> !l.isEmpty()).forEach(l -> {
                        try (Connection connection = dataSource.getConnection()) {
                            try (Statement statement = connection.createStatement()) {
                                statement.executeUpdate(l);
                            }
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                }
            }
        } finally {
            if (serverControl != null) {
                serverControl.shutdown();
            }
        }
    }

    private void turnOnBuiltInUsers(final Connection conn, final String password) throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Setting and Confirming requireAuthentication
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.connection.requireAuthentication', 'true')");
            // Setting authentication scheme to Derby
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.authentication.provider', 'BUILTIN')");
            // Creating some sample users
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.sa', '" + password + "')");
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.guest', '" + password + "')");
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.admin', '" + password + "')");
            // Setting default connection mode to no access
            s.executeUpdate(
                    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.defaultConnectionMode', 'noAccess')");

            // Defining read-write users
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.fullAccessUsers', 'sa,admin')");

            // Defining read-only users
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.readOnlyAccessUsers', 'guest')");

            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.propertiesOnly', 'false')");
        }
    }

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {

        return DerbyInfo.class == parameterContext.getParameter().getType();
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {

        if (DerbyInfo.class == parameterContext.getParameter().getType()) {
            return derbyDb;
        }

        return null;
    }

    @Data
    public static class DerbyInfo {

        private final String server;

        private final int port;

        private final String dbName;
    }
}
