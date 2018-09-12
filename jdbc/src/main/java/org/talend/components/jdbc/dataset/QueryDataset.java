package org.talend.components.jdbc.dataset;

import java.io.Serializable;

import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row("sourceType"), @GridLayout.Row("tableName"),
        @GridLayout.Row("sqlQuery"), })
@DataSet("query.selectonly")
@Documentation("A read only query to a database")
public class QueryDataset implements Serializable {

    @Option
    @Documentation("the connection information to execute the query")
    private BasicDatastore connection;

    @Option
    @Documentation("The source type")
    private SourceType sourceType = SourceType.QUERY;

    @Option
    @Documentation("The table name if the source type is a TABLE")
    @ActiveIf(target = "sourceType", value = { "TABLE_NAME" })
    private String tableName;

    @Option
    @ActiveIf(target = "sourceType", value = { "QUERY" })
    @Documentation("A valid read only query is the source type is Query")
    private String sqlQuery;

    public enum SourceType {
        TABLE_NAME,
        QUERY
    }

}
