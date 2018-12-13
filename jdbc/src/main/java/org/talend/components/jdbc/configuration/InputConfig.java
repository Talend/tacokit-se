package org.talend.components.jdbc.configuration;

import org.talend.components.jdbc.dataset.BaseDataSet;

import java.io.Serializable;

public interface InputConfig extends Serializable {

    BaseDataSet getDataSet();

    int getFetchSize();

}
