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

import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACTION;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_BY;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DELETE_BY;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;

import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.OutputAction;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.CustomObjectClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;

public class CustomObjectStrategy extends OutputComponentStrategy {

    private CustomObjectClient customObjectClient;

    private String customObjectName;

    public CustomObjectStrategy(@Option("configuration") final MarketoOutputDataSet dataSet, final I18nMessage i18n,
            final AuthorizationClient authorizationClient, final JsonBuilderFactory jsonFactory,
            final JsonReaderFactory jsonReader, final JsonWriterFactory jsonWriter, final CustomObjectClient customObjectClient) {
        super(dataSet, i18n, authorizationClient, jsonFactory, jsonReader, jsonWriter);
        this.customObjectClient = customObjectClient;
        this.customObjectClient.base(dataSet.getDataStore().getEndpoint());
    }

    @Override
    public JsonObject runAction(JsonObject payload) {
        customObjectName = dataSet.getCustomObjectName();
        switch (dataSet.getAction()) {
        case sync:
            return syncCustomObject(payload);
        case delete:
            return deleteCustomObject(payload);
        }
        throw new UnsupportedOperationException(i18n.invalidOperation());
    }

    @Override
    public JsonObject getPayload(JsonObject incomingData) {
        JsonObject data = incomingData;
        JsonArray input = jsonFactory.createArrayBuilder().add(data).build();
        if (OutputAction.sync.equals(dataSet.getAction())) {
            return jsonFactory.createObjectBuilder() //
                    .add(ATTR_ACTION, dataSet.getSyncMethod().name()) //
                    .add(ATTR_DEDUPE_BY, dataSet.getDedupeBy()) //
                    .add(ATTR_INPUT, input) //
                    .build();
        } else {
            return jsonFactory.createObjectBuilder() //
                    .add(ATTR_DELETE_BY, dataSet.getDeleteBy().name()) //
                    .add(ATTR_INPUT, input) //
                    .build();
        }
    }

    private JsonObject syncCustomObject(JsonObject payload) {
        return handleResponse(customObjectClient.syncCustomObjects(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken,
                customObjectName, payload));
    }

    private JsonObject deleteCustomObject(JsonObject payload) {
        return handleResponse(customObjectClient.deleteCustomObjects(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken,
                customObjectName, payload));
    }

}
