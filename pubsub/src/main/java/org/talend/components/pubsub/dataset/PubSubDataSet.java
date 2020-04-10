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
package org.talend.components.pubsub.dataset;

import lombok.Data;
import lombok.Getter;
import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.components.pubsub.service.PubSubService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "pubsub")
@DataSet("PubSubDataSet")
@GridLayout({ //
        @GridLayout.Row("dataStore"), @GridLayout.Row("topic"), @GridLayout.Row("subscription"), @GridLayout.Row("valueFormat"),
        @GridLayout.Row("fieldDelimiter"), @GridLayout.Row("otherDelimiter"), @GridLayout.Row("avroSchema"),
        @GridLayout.Row("pathToText") })
@Documentation("Pub/Sub Dataset Properties")
public class PubSubDataSet implements Serializable {

    @Option
    @Documentation("Connection")
    private PubSubDataStore dataStore;

    @Option
    @Required
    @Suggestable(value = PubSubService.ACTION_SUGGESTION_TOPICS, parameters = "dataStore")
    @Documentation("Topic name")
    private String topic;

    @Option
    @Suggestable(value = PubSubService.ACTION_SUGGESTION_SUBSCRIPTIONS, parameters = { "dataStore", "topic" })
    @Documentation("Subscription id. If left blank, a unique subscription id will be generated.")
    private String subscription;

    @Option
    @Required
    @DefaultValue(value = "CSV")
    @Documentation("Value format")
    private ValueFormat valueFormat;

    @Option
    @DefaultValue(value = "SEMICOLON")
    @ActiveIf(target = "valueFormat", value = { "CSV" })
    @Documentation("Field delimiter for CSV")
    private CSVDelimiter fieldDelimiter;

    @Option
    @DefaultValue(value = ";")
    @ActiveIfs(operator = ActiveIfs.Operator.AND, value = { @ActiveIf(target = "fieldDelimiter", value = { "OTHER" }),
            @ActiveIf(target = "valueFormat", value = { "CSV" }) })
    @Documentation("Other field delimiter for CSV")
    private Character otherDelimiter;

    @Option
    @Code(value = "json")
    @ActiveIf(target = "valueFormat", value = { "AVRO" })
    @Documentation("Avro schema")
    private String avroSchema;

    @Option
    @ActiveIf(target = "valueFormat", value = { "TEXT" })
    @Pattern("\\/.*[^\\/]")
    @Documentation("Path to text field in record (output only) : Json pointer format")
    private String pathToText;

    public enum ValueFormat {
        CSV,
        JSON,
        AVRO,
        TEXT
    }

    public enum CSVDelimiter {
        COMMA(','),
        SEMICOLON(';'),
        TAB('\t'),
        SPACE(' '),
        OTHER('\u0000');

        @Getter
        private final char value;

        CSVDelimiter(char value) {
            this.value = value;
        }
    }

}