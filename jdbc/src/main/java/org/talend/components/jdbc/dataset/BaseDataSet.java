package org.talend.components.jdbc.dataset;

import org.talend.components.jdbc.datastore.JdbcConnection;

import java.io.Serializable;

public interface BaseDataSet extends Serializable {

    JdbcConnection getConnection();

    String getQuery();

}
