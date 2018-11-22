package org.talend.components.jdbc;

public interface JdbcContainer {

    String getDatabaseType();

    String getUsername();

    String getPassword();

    String getJdbcUrl();

}
