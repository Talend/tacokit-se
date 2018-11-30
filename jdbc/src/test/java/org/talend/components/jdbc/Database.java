package org.talend.components.jdbc;

import java.util.stream.Stream;

import static java.lang.Boolean.getBoolean;
import static java.util.Locale.ROOT;

public enum Database {

    ALL,
    DERBY,
    MARIADB,
    MSSQL,
    MYSQL,
    ORACLE,
    POSTGRESQL,
    SNOWFLAKE;

    private static final String ENV_PREFIX = "talend.jdbc.it.";

    public static Stream<Database> getActiveDatabases() {
        if (getBoolean(ENV_PREFIX + ALL.name().toLowerCase(ROOT) + ".skip")) {
            return Stream.empty();
        }

        return getBoolean("talend.jdbc.it") ? Stream.of(values()).filter(db -> db != ALL)
                .filter(db -> !getBoolean(ENV_PREFIX + db.name().toLowerCase(ROOT) + ".skip")) : Stream.of(DERBY);
    }
}
