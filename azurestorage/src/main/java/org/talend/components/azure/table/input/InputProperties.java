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
package org.talend.components.azure.table.input;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.talend.components.azure.common.AzureTableConnection;
import org.talend.components.azure.common.Comparison;
import org.talend.components.azure.common.NameMapping;
import org.talend.components.azure.service.AzureComponentServices;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import com.microsoft.azure.storage.table.EdmType;
import com.microsoft.azure.storage.table.TableQuery;
import lombok.Data;
import static org.talend.components.azure.service.AzureComponentServices.COLUMN_NAMES;

@GridLayout(value = { @GridLayout.Row("azureConnection"), @GridLayout.Row("useFilterExpression"),
        @GridLayout.Row("filterExpressions"), @GridLayout.Row("dieOnError"),
        @GridLayout.Row("schema") }, names = GridLayout.FormType.MAIN)

@GridLayout(value = { @GridLayout.Row("nameMappings") }, names = GridLayout.FormType.ADVANCED)
@Documentation("These properties are used to configure AzureStorageTableInput")
@Data
public class InputProperties implements Serializable {

    @Option
    @Documentation("Azure Table Connection")
    private AzureTableConnection azureConnection;

    @Option
    @Documentation("Use conditions to filter the entities to be retrieved")
    private boolean useFilterExpression;

    @Option
    @Documentation("Conditions to filter the entities to be retrieved")
    @ActiveIf(target = "useFilterExpression", value = "true")
    private List<FilterExpression> filterExpressions;

    @Option
    @Documentation("Stop the execution of the Job when an error occurs.")
    private boolean dieOnError = true;

    @Option
    @Documentation("Mapping the column name of the component schema with the property name of the Azure table entity if they are different.")
    private List<NameMapping> nameMappings;

    @Option
    @Structure(discoverSchema = AzureComponentServices.GUESS_SCHEMA, type = Structure.Type.OUT)
    @Documentation("A schema is a row description. It defines the number of fields (columns) to be processed and passed on to the next component.")
    private List<String> schema;

    public enum Predicate {
        AND("AND", TableQuery.Operators.AND),
        OR("OR", TableQuery.Operators.OR);

        private String displayName;

        private String operator;

        private static Map<String, Predicate> mapPossibleValues = new HashMap<>();

        static {
            for (Predicate predicate : values()) {
                mapPossibleValues.put(predicate.displayName, predicate);
            }
        }

        Predicate(String displayName, String operator) {
            this.displayName = displayName;
            this.operator = operator;
        }

        /**
         * Convert String predicate to Azure Type {@link TableQuery.Operators}
         */
        public static String getOperator(String p) {

            if (!mapPossibleValues.containsKey(p)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value %s, it must be %s", p, mapPossibleValues.keySet()));
            }
            return mapPossibleValues.get(p).operator;
        }

        @Override
        public String toString() {
            return this.displayName;
        }
    }

    public enum FieldType {
        STRING("STRING", EdmType.STRING),

        NUMERIC("NUMERIC", EdmType.INT32),

        DATE("DATE", EdmType.DATE_TIME),

        GUID("GUID", EdmType.GUID),

        BOOLEAN("BOOLEAN", EdmType.BOOLEAN);

        private String displayName;

        private EdmType supportedType;

        private static Map<String, FieldType> mapPossibleValues = new HashMap<>();

        private static List<String> possibleValues = new ArrayList<>();

        static {
            for (FieldType supportedFieldType : values()) {
                possibleValues.add(supportedFieldType.displayName);
                mapPossibleValues.put(supportedFieldType.displayName, supportedFieldType);
            }
        }

        FieldType(String displayName, EdmType supportedType) {
            this.displayName = displayName;
            this.supportedType = supportedType;
        }

        /**
         * Convert String type names to Azure Type {@link EdmType}
         */
        public static EdmType getEdmType(String ft) {
            if (!mapPossibleValues.containsKey(ft)) {
                throw new IllegalArgumentException(String.format("Invalid value %s, it must be %s", ft, possibleValues));
            }
            return mapPossibleValues.get(ft).supportedType;
        }

        @Override
        public String toString() {
            return this.displayName;
        }

    }

    @Data
    @OptionsOrder({ "column", "function", "value", "predicate", "fieldType" })
    public static class FilterExpression implements Serializable {

        @Option
        @Documentation("Name of the property on which you want to apply for the condition.")
        @Suggestable(value = COLUMN_NAMES, parameters = "../../schema")
        private String column;

        @Option
        @Documentation("The comparison operator you want to use.")
        private Comparison function = Comparison.EQUAL;

        @Option
        @Documentation("The value used to compare the property to")
        private String value;

        @Option
        @Documentation("The predicate used to combine the conditions. Use any if you have only one condition")
        private Predicate predicate = Predicate.AND;

        @Option
        @Documentation("Type of the column ")
        private FieldType fieldType = FieldType.STRING;
    }
}
