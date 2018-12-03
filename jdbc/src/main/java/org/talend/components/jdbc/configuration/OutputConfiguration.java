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
package org.talend.components.jdbc.configuration;

import lombok.Data;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.talend.sdk.component.api.configuration.condition.ActiveIf.EvaluationStrategy.CONTAINS;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.OR;

@Data
@GridLayout(value = { @GridLayout.Row("dataset"), @GridLayout.Row("createTableIfNotExists"), @GridLayout.Row({ "actionOnData" }),
        @GridLayout.Row("keys"), @GridLayout.Row("ignoreUpdate") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("rewriteBatchedStatements") })
@Documentation("Those properties define an output data set for the JDBC output component")
public class OutputConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("Dataset configuration")
    private TableNameDataset dataset;

    @Option
    @Required
    @ActiveIf(target = "actionOnData", value = { "INSERT", "UPSERT" })
    @Documentation("Create table if don't exists")
    private boolean createTableIfNotExists = false;

    @Option
    @Documentation("The action to be performed")
    private ActionOnData actionOnData = ActionOnData.INSERT;

    @Option
    @ActiveIfs(operator = OR, value = { @ActiveIf(target = "actionOnData", negate = true, value = { "INSERT" }),
            @ActiveIf(target = "createTableIfNotExists", value = { "true" }), })
    // fixme activate when https://jira.talendforge.org/browse/TFD-5995 is fixed
    // @Suggestable(value = ACTION_SUGGESTION_TABLE_COLUMNS_NAMES, parameters = { "../dataset" })
    @Documentation("List of columns to be used as keys for this operation")
    private List<String> keys = new ArrayList<>();

    @Option
    // fixme activate when https://jira.talendforge.org/browse/TFD-5995 is fixed
    // @Suggestable(value = ACTION_SUGGESTION_TABLE_COLUMNS_NAMES, parameters = { "../dataset" })
    @ActiveIf(target = "actionOnData", value = { "UPDATE", "UPSERT" })
    @Documentation("List of columns to be ignored from update")
    private List<String> ignoreUpdate = new ArrayList<>();

    @Option
    @ActiveIfs(operator = OR, value = { @ActiveIf(target = "../dataset.connection.dbType", value = { "MySQL" }),
            @ActiveIf(target = "../dataset.connection.handler", evaluationStrategy = CONTAINS, value = { "MySQL" }) })
    @Documentation("Rewrite batched statements, to execute one statement per batch combining values in the sql query")
    private boolean rewriteBatchedStatements = true;

    public enum ActionOnData {
        INSERT,
        UPDATE,
        DELETE,
        UPSERT
    }
}
