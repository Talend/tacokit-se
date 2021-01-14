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
package org.talend.components.bigquery.input;

import lombok.Data;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.BIGQUERY;

@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "bigquery")
@Documentation("Dataset of a BigQuery for Input.")
@GridLayout({ @GridLayout.Row("tableDataset") })
public class BigQueryTableExtractInputConfig implements Serializable {

    @Option
    @Documentation("BigQuery Table Dataset")
    private TableDataSet tableDataset;

    public BigQueryConnection getDataStore() {
        return tableDataset.getConnection();
    }

}
