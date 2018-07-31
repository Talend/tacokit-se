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
package org.talend.components.marketo.output;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;

import javax.annotation.PostConstruct;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.CompanyClient;
import org.talend.components.marketo.service.CustomObjectClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.components.marketo.service.LeadClient;
import org.talend.components.marketo.service.ListClient;
import org.talend.components.marketo.service.OpportunityClient;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

@Version
@Processor(family = "Marketo", name = "Output")
@Icon(value = Icon.IconType.CUSTOM, custom = "MarketoOutput")
@Documentation("Marketo Output Component")
public class MarketoProcessor extends MarketoSourceOrProcessor {

    protected final MarketoOutputDataSet dataSet;

    private ProcessorStrategy strategy;

    private transient static final Logger LOG = LoggerFactory.getLogger(MarketoProcessor.class);

    public MarketoProcessor(@Option("configuration") final MarketoOutputDataSet dataSet, //
            final I18nMessage i18n, //
            final JsonBuilderFactory jsonFactory, //
            final JsonReaderFactory jsonReader, //
            final JsonWriterFactory jsonWriter, //
            // REST API Clients
            final AuthorizationClient authorizationClient, //
            final LeadClient leadClient, //
            final ListClient listClient, //
            final CompanyClient companyClient, //
            final CustomObjectClient customObjectClient, //
            final OpportunityClient opportunityClient) {
        super(dataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient);
        this.dataSet = dataSet;

        switch (dataSet.getEntity()) {
        case Lead:
            strategy = new LeadStrategy(dataSet, i18n, authorizationClient, jsonFactory, jsonReader, jsonWriter, leadClient);
            break;
        case List:
            strategy = new ListStrategy(dataSet, i18n, authorizationClient, jsonFactory, jsonReader, jsonWriter, listClient);
            break;
        case CustomObject:
            strategy = new CustomObjectStrategy(dataSet, i18n, authorizationClient, jsonFactory, jsonReader, jsonWriter,
                    customObjectClient);
            break;
        case Company:
            strategy = new CompanyStrategy(dataSet, i18n, authorizationClient, jsonFactory, jsonReader, jsonWriter,
                    companyClient);
            break;
        case Opportunity:
        case OpportunityRole:
            strategy = new OpportunityStrategy(dataSet, i18n, authorizationClient, jsonFactory, jsonReader, jsonWriter,
                    opportunityClient);
            break;
        }
    }

    @PostConstruct
    @Override
    public void init() {
        strategy.init();
    }

    @ElementListener
    public void map(final JsonObject data, @Output final OutputEmitter<JsonObject> main,
            @Output("rejected") final OutputEmitter<JsonObject> rejected) {
        LOG.debug("[map] received: {}.", data);
        JsonObject payload = strategy.getPayload(data);
        LOG.debug("[map] payload : {}.", payload);
        JsonObject result = strategy.runAction(payload);
        LOG.debug("[map] result  : {}.", result);
        for (JsonObject status : result.getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class)) {
            if (strategy.isRejected(status)) {
                rejected.emit(strategy.createRejectData(status));
            } else {
                main.emit(strategy.createMainData(status));
            }
        }
    }

    public void mapWithIndexedRecord(final IndexedRecord data, @Output final OutputEmitter<IndexedRecord> main,
            @Output("rejected") final OutputEmitter<IndexedRecord> rejected) {
        JsonObject payload = strategy.getPayload(toJson(data));
        JsonObject result = strategy.runAction(payload);
        for (JsonObject status : result.getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class)) {
            if (strategy.isRejected(status)) {
                rejected.emit(toIndexedRecord(strategy.createRejectData(status), null));
            } else {
                main.emit(toIndexedRecord(strategy.createMainData(status), null));
            }
        }
    }

}
