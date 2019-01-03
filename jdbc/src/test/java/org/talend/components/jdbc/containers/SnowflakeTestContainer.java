/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.containers;

import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

public class SnowflakeTestContainer implements JdbcTestContainer {

    private final static String SERVER_ID = "snowflake-for-test";

    private final static String TEST_ACCOUNT = "talend";

    private final static String TEST_WAREHOUSE = "JDBC_IT_WAREHOUSE";

    private final static String TEST_DATABASE = "JDBC_IT_DATABASE";

    private Server server;

    public SnowflakeTestContainer() {
        server = new MavenDecrypter().find(SERVER_ID);
        if (server == null) {
            throw new IllegalArgumentException("cant find snowflake server configuration. Please configure a server with id "
                    + SERVER_ID + " and snowflake username/password for integration test in the maven settings.xml file");
        }
    }

    @Override
    public String getDatabaseType() {
        return "Snowflake";
    }

    @Override
    public String getUsername() {
        return server.getUsername();
    }

    @Override
    public String getPassword() {
        return server.getPassword();
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:snowflake://" + TEST_ACCOUNT + ".snowflakecomputing.com/?schema=PUBLIC&db=" + TEST_DATABASE + "&warehouse="
                + TEST_WAREHOUSE;
    }

    @Override
    public void start() {
        // no-op cloud env
    }

    @Override
    public void stop() {
        // no-op cloud env
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }
}
