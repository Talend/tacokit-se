package org.talend.components.jdbc.dataset;

import lombok.Data;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import static org.talend.components.jdbc.service.UIActionService.ACTION_SUGGESTION_TABLE_NAMES;

@Data
@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row("tableName") })
@DataSet("TableNameDataset")
@Documentation("This configuration define a dataset using a database table name.\nIt's generate a select * from table query")
public class TableNameDataset implements BaseDataSet {

    @Option
    @Documentation("the connection information to execute the query")
    private JdbcConnection connection;

    @Option
    @Required
    @Documentation("The table name")
    @Suggestable(value = ACTION_SUGGESTION_TABLE_NAMES, parameters = "connection")
    private String tableName;

    @Override
    public String getQuery() {
        return "select * from " + PlatformFactory.get(connection).identifier(getTableName());
    }
}
