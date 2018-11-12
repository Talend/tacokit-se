package org.talend.components.jdbc.dataset;

import java.io.Serializable;

import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row("tableName") })
@DataSet("tableName")
@Documentation("A read only query to a database")
public class TableNameDataset implements BaseDataSet, Serializable {

    @Option
    @Documentation("the connection information to execute the query")
    private BasicDatastore connection;

    @Option
    @Required
    @Documentation("The table name if the source type is a TABLE")
    @Suggestable(value = "tables.list", parameters = "connection")
    private String tableName;

    @Override
    public String getQuery() {
        return "select * from " + getTableName();
    }
}
