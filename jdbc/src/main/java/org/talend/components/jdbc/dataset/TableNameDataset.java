/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.dataset;

import lombok.Data;
import lombok.experimental.Delegate;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import static java.util.Optional.ofNullable;
import static org.talend.components.jdbc.output.platforms.PlatformFactory.get;
import static org.talend.components.jdbc.service.UIActionService.ACTION_SUGGESTION_TABLE_NAMES;
import static org.talend.components.jdbc.service.UIActionService.ACTION_LIST_COLUMNS;
import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

import java.util.List;
import java.util.stream.Collectors;

@Data
@DataSet("TableNameDataset")
@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row("tableName"), @GridLayout.Row("listColumns") })
@GridLayout(names = ADVANCED, value = { @GridLayout.Row("connection"), @GridLayout.Row("advancedCommon") })
@Documentation("This configuration define a dataset using a database table name.\nIt's generate a select * from table query")
public class TableNameDataset implements BaseDataSet {

    private static final String QUERY_TEMPLATE = "select %s from %s";

    @Option
    @Documentation("the connection information to execute the query")
    private JdbcConnection connection;

    @Option
    @Required
    @Documentation("The table name")
    @Suggestable(value = ACTION_SUGGESTION_TABLE_NAMES, parameters = "connection")
    private String tableName;

    @Option
    @Suggestable(value = ACTION_LIST_COLUMNS, parameters = { "connection", "tableName" })
    @Documentation("Selected column names.")
    private List<String> listColumns;

    @Option
    @Delegate
    @Documentation("common input configuration")
    private AdvancedCommon advancedCommon = new AdvancedCommon();

    @Override
    public String getQuery() {
        Platform platform = get(connection, null);
        String columns = ofNullable(getListColumns()).filter(list -> !list.isEmpty())
                .map(l -> l.stream().map(platform::identifier).collect(Collectors.joining(","))).orElse("*");
        // No need for the i18n service for this instance
        return String.format(QUERY_TEMPLATE, columns, platform.identifier(getTableName()));
    }
}
