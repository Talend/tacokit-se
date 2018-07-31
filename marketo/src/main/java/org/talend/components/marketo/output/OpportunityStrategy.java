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

import static org.slf4j.LoggerFactory.getLogger;
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

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.OutputAction;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.components.marketo.service.OpportunityClient;
import org.talend.sdk.component.api.configuration.Option;

public class OpportunityStrategy extends OutputComponentStrategy {

    private OpportunityClient opportunityClient;

    private boolean isOpportunityRole;

    private transient static final Logger LOG = getLogger(OpportunityStrategy.class);

    public OpportunityStrategy(@Option("configuration") final MarketoOutputDataSet dataSet, final I18nMessage i18n,
            final AuthorizationClient authorizationClient, final JsonBuilderFactory jsonFactory,
            final JsonReaderFactory jsonReader, final JsonWriterFactory jsonWriter, final OpportunityClient opportunityClient) {
        super(dataSet, i18n, authorizationClient, jsonFactory, jsonReader, jsonWriter);
        this.opportunityClient = opportunityClient;
        this.opportunityClient.base(dataSet.getDataStore().getEndpoint());
        isOpportunityRole = MarketoEntity.OpportunityRole.equals(dataSet.getEntity());
    }

    @Override
    public JsonObject getPayload(JsonObject incomingData) {
        JsonObject data = incomingData;
        JsonArray input = jsonFactory.createArrayBuilder().add(data).build();
        LOG.warn("[getPayload] data: {}; input: {}.", incomingData, input);

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

    @Override
    public JsonObject runAction(JsonObject payload) {
        switch (dataSet.getAction()) {
        case sync:
            return syncOpportunity(payload);
        case delete:
            return deleteOpportunity(payload);
        }
        throw new UnsupportedOperationException(i18n.invalidOperation());
    }

    private JsonObject syncOpportunity(JsonObject payload) {
        if (isOpportunityRole) {
            return handleResponse(
                    opportunityClient.syncOpportunityRoles(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        } else {
            return handleResponse(
                    opportunityClient.syncOpportunities(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        }
    }

    private JsonObject deleteOpportunity(JsonObject payload) {
        if (isOpportunityRole) {
            return handleResponse(
                    opportunityClient.deleteOpportunityRoles(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        } else {
            return handleResponse(
                    opportunityClient.deleteOpportunities(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        }
    }

}
