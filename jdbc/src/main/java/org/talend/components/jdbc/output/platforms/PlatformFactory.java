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
package org.talend.components.jdbc.output.platforms;

import org.talend.components.jdbc.datastore.JdbcConnection;

import java.util.Locale;

import static java.util.Optional.ofNullable;
import static org.talend.components.jdbc.output.platforms.DerbyPlatform.DERBY;
import static org.talend.components.jdbc.output.platforms.MSSQLPlatform.MSSQL;
import static org.talend.components.jdbc.output.platforms.MariaDbPlatform.MARIADB;
import static org.talend.components.jdbc.output.platforms.MySQLPlatform.MYSQL;
import static org.talend.components.jdbc.output.platforms.OraclePlatform.ORACLE;
import static org.talend.components.jdbc.output.platforms.PostgreSQLPlatform.POSTGRESQL;
import static org.talend.components.jdbc.output.platforms.RedshiftPlatform.REDSHIFT;
import static org.talend.components.jdbc.output.platforms.SnowflakePlatform.SNOWFLAKE;

public final class PlatformFactory {

    public static Platform get(final JdbcConnection connection) {
        final String dbType = ofNullable(connection.getHandler()).orElseGet(connection::getDbType);
        switch (dbType.toLowerCase(Locale.ROOT)) {
        case MYSQL:
            return new MySQLPlatform();
        case MARIADB:
            return new MariaDbPlatform();
        case POSTGRESQL:
            return new PostgreSQLPlatform();
        case REDSHIFT:
            return new RedshiftPlatform();
        case SNOWFLAKE:
            return new SnowflakePlatform();
        case ORACLE:
            return new OraclePlatform();
        case MSSQL:
            return new MSSQLPlatform();
        case DERBY:
            return new DerbyPlatform();
        default:
            throw new RuntimeException("unsupported database " + dbType);
        }
    }

    private PlatformFactory() {
        // no-op
    }
}
