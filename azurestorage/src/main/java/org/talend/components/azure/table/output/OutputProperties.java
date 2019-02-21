/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.azure.table.output;

import java.io.Serializable;
import java.util.List;

import org.talend.components.azure.common.AzureTableConnection;
import org.talend.components.azure.common.NameMapping;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import static org.talend.components.azure.service.AzureComponentServices.COLUMN_NAMES;
import static org.talend.components.azure.service.AzureComponentServices.SUPPORTED_ACTION_ON_TABLE;

@GridLayout(value = { @GridLayout.Row("azureConnection"), @GridLayout.Row({ "partitionName", "rowKey" }),
        @GridLayout.Row({ "actionOnData", "actionOnTable" }), @GridLayout.Row("processInBatch"), @GridLayout.Row("dieOnError"),
        @GridLayout.Row("schema")

}, names = GridLayout.FormType.MAIN)

@GridLayout(value = { @GridLayout.Row("nameMappings") }, names = GridLayout.FormType.ADVANCED)
@Documentation("These properties are used to configure AzureStorageTableOutput")
@Data
public class OutputProperties implements Serializable {

    @Option
    @Documentation("Azure Table Connection")
    private AzureTableConnection azureConnection;

    @Option
    @Suggestable(value = COLUMN_NAMES, parameters = "schema")
    @Documentation("Schema column that holds the partition key value")
    private String partitionName;

    @Option
    @Documentation("Schema column that holds the row key value")
    @Suggestable(value = COLUMN_NAMES, parameters = "schema")
    private String rowKey;

    @Option
    @Documentation("An action to be performed on data of the table defined")
    private ActionOnData actionOnData = ActionOnData.INSERT;

    @Option
    @Proposable(SUPPORTED_ACTION_ON_TABLE)
    @Documentation("An operation to be performed on the table defined")
    private String actionOnTable = ActionOnTable.DEFAULT.toString();

    @Option
    @Documentation("Process the input entities in batch.\n"
            + "Note that the entities to be processed in batch should belong to the same partition group, "
            + "which means, they should have the same partition key value.")
    private boolean processInBatch;

    @Option
    @Documentation("Stop the execution of the Job when an error occurs")
    private boolean dieOnError = true;

    @Option
    @Documentation("Mapping the column name of the component schema with the property name of the Azure table entity if they are different.")
    private List<NameMapping> nameMappings;

    @Option
    @Structure
    @Documentation("A schema is a row description. It defines the number of fields (columns) to be processed and passed on to the next component.")
    private List<String> schema;

    enum ActionOnData {
        INSERT,
        INSERT_OR_REPLACE,
        INSERT_OR_MERGE,
        MERGE,
        REPLACE,
        DELETE
    }

    public enum ActionOnTable {
        DEFAULT,
        DROP_AND_CREATE,
        CREATE,
        CREATE_IF_NOT_EXIST,
        DROP_IF_EXIST_CREATE
    }
}