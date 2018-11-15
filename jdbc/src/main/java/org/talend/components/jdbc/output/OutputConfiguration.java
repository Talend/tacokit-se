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
package org.talend.components.jdbc.output;

import static org.talend.components.jdbc.service.ActionService.ACTION_SUGGESTION_TABLE_COLUMNS_NAMES;

import java.io.Serializable;
import java.util.List;

import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.AutoLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@GridLayout(value = { @GridLayout.Row("dataset"), @GridLayout.Row({ "actionOnData" }), @GridLayout.Row("updateOperationMapping"),
        @GridLayout.Row("deleteKeys") })
@Documentation("Those properties define an output data set for the JDBC output component.")
public class OutputConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("Dataset configuration")
    private TableNameDataset dataset;

    @Option
    @Documentation("The action to be performed")
    private ActionOnData actionOnData = ActionOnData.INSERT;

    @Option
    @ActiveIf(target = "actionOnData", value = "UPDATE")
    @Documentation("The update operation mapping. This mapping indicate the columns to be used as keys for the update. Columns that are not marked as key will be updated.")
    private List<UpdateOperationMapping> updateOperationMapping;

    @Option
    @ActiveIf(target = "actionOnData", value = "DELETE")
    @Documentation("The keys to be used in the where clause of the delete query. those keys need to be a part of the record columns")
    private List<String> deleteKeys;

    public enum ActionOnData {
        INSERT,
        UPDATE,
        DELETE
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @AutoLayout
    public static class UpdateOperationMapping implements Serializable {

        @Option
        @Suggestable(value = ACTION_SUGGESTION_TABLE_COLUMNS_NAMES, parameters = { "../../dataset" })
        @Documentation("The column name")
        private String column;

        @Option
        @Documentation("Is the column a key")
        private boolean key;
    }

}
