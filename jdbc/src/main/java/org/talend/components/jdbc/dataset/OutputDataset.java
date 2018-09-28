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
import java.util.List;

import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.AutoLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@DataSet("JdbcOutputDataset")
@GridLayout(value = { @GridLayout.Row("connection"), @GridLayout.Row({ "tableName" }), @GridLayout.Row({ "actionOnData" }),
        @GridLayout.Row("updateOperationMapping"), @GridLayout.Row("deleteKeys"), })
@Documentation("Those properties define an output data set for the JDBC output component.")
public class OutputDataset implements Serializable {

    @Option
    @Documentation("")
    private BasicDatastore connection;

    @Option
    @Suggestable(value = "tables.list", parameters = "connection")
    @Documentation("")
    private String tableName;

    @Option
    @Documentation("")
    private ActionOnData actionOnData = ActionOnData.Insert;

    @Option
    @ActiveIf(target = "actionOnData", value = "Update")
    @Documentation("")
    private List<UpdateOperationMapping> updateOperationMapping;

    @Option
    @ActiveIf(target = "actionOnData", value = "Delete")
    @Documentation("")
    private List<String> deleteKeys;

    public enum ActionOnData {
        Insert,
        Update,
        Delete
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @AutoLayout
    public static class UpdateOperationMapping {

        // fixme : use the values from schema when available in tacokit
        @Option
        @Documentation("")
        private String column;

        @Option
        @Documentation("")
        private boolean key;
    }

}
