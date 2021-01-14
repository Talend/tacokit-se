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
package org.talend.components.bigquery.dataset;

import lombok.Data;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "bigquery")
@DataSet("BigQueryDataSetQueryType")
@Documentation("Dataset of a BigQuery component with query type.")
@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row({ "query", "useLegacySql" }) })
public class QueryDataSet implements Serializable {

    @Option
    @Documentation("The BigQuery connection")
    private BigQueryConnection connection;

    @Option
    @Code("sql")
    @Documentation("The BigQuery query")
    private String query;

    @Option
    @Documentation("Should the query use legacy SQL")
    private boolean useLegacySql;

}