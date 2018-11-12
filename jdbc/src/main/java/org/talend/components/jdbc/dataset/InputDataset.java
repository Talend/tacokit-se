/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

import java.io.Serializable;

import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
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
public class InputDataset implements Serializable {

    @Option
    @Documentation("the connection information to execute the query")
    private BasicDatastore connection;

    @Option
    @Documentation("The source type")
    private SourceType sourceType = SourceType.QUERY;

    @Option
    @Documentation("The table name if the source type is a TABLE")
    @Suggestable(value = "tables.list", parameters = "connection")
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
