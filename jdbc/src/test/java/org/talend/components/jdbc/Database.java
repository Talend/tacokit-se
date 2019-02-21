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

    public static Stream<Database> getActiveDatabases() {
        return Stream.of(values()).filter(db -> db != ALL);
    }
}
