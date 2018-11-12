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
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row("sqlQuery") })
@DataSet("query")
@Documentation("A read only query to a database")
public class QueryDataset implements BaseDataSet, Serializable {

    @Option
    @Documentation("The connection information to execute the query")
    private BasicDatastore connection;

    @Option
    @Documentation("A valid read only query is the source type is Query")
    private String sqlQuery;

    @Override
    public String getQuery() {
        return sqlQuery;
    }
}
