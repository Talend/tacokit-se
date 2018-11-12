package org.talend.components.jdbc.dataset;

import org.talend.components.jdbc.datastore.BasicDatastore;

public interface BaseDataSet {

    BasicDatastore getConnection();

    String getQuery();

}
