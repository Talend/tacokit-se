package org.talend.components.jdbc.dataset;

import org.talend.components.jdbc.datastore.BasicDatastore;

import java.io.Serializable;

public interface BaseDataSet extends Serializable {

    BasicDatastore getConnection();

    String getQuery();

}
