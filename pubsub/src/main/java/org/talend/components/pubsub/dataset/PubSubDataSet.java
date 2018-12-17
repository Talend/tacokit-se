// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.pubsub.dataset;

import lombok.Data;

import static org.talend.components.pubsub.service.UIActionService.ACTION_SUGGESTION_SUBSCRIPTIONS;
import static org.talend.components.pubsub.service.UIActionService.ACTION_SUGGESTION_TOPICS;

import java.io.Serializable;

import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet("PubSubDataSet")
@GridLayout({ //
        @GridLayout.Row("dataStore"), //
        @GridLayout.Row("topic"), //
        @GridLayout.Row("subscription"), //
        @GridLayout.Row("valueFormat"), //
        @GridLayout.Row("fieldDelimiter"), //
        @GridLayout.Row("avroSchema"), //
})
@Documentation("TODO")
public class PubSubDataSet implements Serializable {

    @Option
    @Documentation("TODO")
    private PubSubDataStore dataStore;

    @Option
    @Required
    @Suggestable(value = ACTION_SUGGESTION_TOPICS, parameters = "dataStore")
    @Documentation("TODO")
    private String topic;

    @Option
    @Required
    @Suggestable(value = ACTION_SUGGESTION_SUBSCRIPTIONS, parameters = "dataStore")
    @Documentation("TODO")
    private String subscription;

    @Option
    @Required
    @Documentation("TODO")
    private ValueFormat valueFormat;

    @Option
    @DefaultValue(value = ";")
    @ActiveIf(target = "valueFormat", value = { "CSV" })
    @Documentation("TODO")
    private String fieldDelimiter;

    @Option
    @Code(value = "json")
    @ActiveIf(target = "valueFormat", value = { "AVRO" })
    @Documentation("TODO")
    private String avroSchema;

    public enum ValueFormat {
        CSV,
        AVRO
    }

}
