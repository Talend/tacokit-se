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
package org.talend.components.jdbc.output.platforms;

import java.util.Locale;

public final class PlatformFactory {

    public static Platform get(final String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("database name can't be null");
        }
        switch (name.toLowerCase(Locale.ROOT)) {
        case MySQLPlatform.NAME:
            return new MySQLPlatform();
        case MariaDbPlatform.NAME:
            return new MariaDbPlatform();
        case PostgreSQLPlatform.NAME:
            return new PostgreSQLPlatform();
        case RedshiftPlatform.NAME:
            return new RedshiftPlatform();
        case SnowflakePlatform.NAME:
            return new SnowflakePlatform();
        case OraclePlatform.NAME:
            return new OraclePlatform();
        case MSSQLPlatform.NAME:
            return new MSSQLPlatform();
        case DerbyPlatform.NAME:
            return new DerbyPlatform();
        default:
            throw new RuntimeException("unsupported database " + name);
        }
    }

    private PlatformFactory() {

    }
}
