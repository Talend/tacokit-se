/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.bigquery.output;

import lombok.Data;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.BIGQUERY;

@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "bigquery")
@Documentation("Dataset of a BigQuery component.")
@OptionsOrder({ "dataSet", "tableOperation" })
public class BigQueryOutputConfig implements Serializable {

    @Option
    @Documentation("BigQuery Dataset")
    private TableDataSet dataSet;

    @Option
    @Documentation("The BigQuery table operation")
    @DefaultValue("NONE")
    private TableOperation tableOperation = TableOperation.NONE;

    public enum TableOperation {
        /**
         * Specifics that tables should not be created.
         *
         * <p>
         * If the output table does not exist, the write fails.
         */
        NONE,
        /**
         * Specifies that tables should be created if needed.
         *
         * <p>
         * When this transformation is executed, if the output table does not exist, the table is created from the
         * provided schema.
         */
        CREATE_IF_NOT_EXISTS,
        /**
         * Specifies that write should replace a table.
         *
         * <p>
         * If the output table does not exist, the write fails.
         *
         */
        TRUNCATE,

        /*
         * Currently not supported by Streams.
         * 
         * /**
         * Specifies that tables should be dropped if exists, and create by the provided schema, which actually the
         * combine with TRUNCATE and CREATE_IF_NOT_EXISTS
         *
         * DROP_IF_EXISTS_AND_CREATE,
         */
    }

}
