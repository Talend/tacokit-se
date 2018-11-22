package org.talend.components.jdbc;

import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.JdbcService;

public interface BaseTest {

    String getTestTableName();

    JdbcConnection newConnection();

    JdbcService getJdbcService();
}
