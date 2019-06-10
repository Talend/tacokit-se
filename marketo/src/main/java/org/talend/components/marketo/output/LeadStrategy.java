// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;

import org.talend.components.marketo.MarketoApiConstants;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.service.LeadClient;
import org.talend.components.marketo.service.ListClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.http.Response;

import lombok.extern.slf4j.Slf4j;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACTION;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ERRORS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LOOKUP_FIELD;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SUCCESS;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;
import static org.talend.components.marketo.dataset.MarketoOutputConfiguration.OutputAction.delete;

@Slf4j
public class LeadStrategy extends OutputComponentStrategy implements ProcessorStrategy {

    private final ListClient listClient;

    private final LeadClient leadClient;

    private transient String listId;

    private transient JsonObject listPayload;

    public LeadStrategy(@Option("configuration") final MarketoOutputConfiguration dataSet, //
            final MarketoService service) {
        super(dataSet, service);
        leadClient = service.getLeadClient();
        listClient = service.getListClient();
        leadClient.base(configuration.getDataSet().getDataStore().getEndpoint());
        listClient.base(configuration.getDataSet().getDataStore().getEndpoint());
    }

    @Override
    public JsonObject getPayload(List<JsonObject> incomingData) {
        JsonArray input = jsonFactory.createArrayBuilder(incomingData).build();
        if (delete.equals(configuration.getAction())) {
            return jsonFactory.createObjectBuilder() //
                    .add(ATTR_INPUT, input) //
                    .build();
        } else {
            // we need to keep leadIds somewhere to update static list after create/update operation
            setLeadsInListPayload(incomingData);
            //
            return jsonFactory.createObjectBuilder() //
                    .add(ATTR_ACTION, configuration.getAction().name()) //
                    .add(ATTR_LOOKUP_FIELD, configuration.getLookupField()) //
                    .add(ATTR_INPUT, input) //
                    .build();
        }
    }

    @Override
    public JsonObject runAction(JsonObject payload) {
        if (configuration.getAction() == delete) {
            return deleteLeads(payload);
        } else {
            return syncLeads(payload);
        }
    }

    private JsonObject deleteLeads(JsonObject payload) {
        return handleResponse(leadClient.deleteLeads(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
    }

    private void setLeadsInListPayload(List<JsonObject> leads) {
        listId = configuration.getDataSet().getListId();
        JsonArrayBuilder builder = jsonFactory.createArrayBuilder();
        leads.stream().map(lead -> builder.add(jsonFactory.createObjectBuilder().add(ATTR_ID, lead.getInt(ATTR_ID))));
        listPayload = jsonFactory.createObjectBuilder().add(ATTR_INPUT, builder.build()).build();
        log.warn("[setLeadsInListPayload] leads in list {} : {}.", listId, listPayload);
    }

    private void handleListResponse(Response<JsonObject> response) {
        log.warn("[handleListResponse] [{}] body: {}.", response.status(), response.body());
        if (response.status() == MarketoApiConstants.HTTP_STATUS_OK && !response.body().getBoolean(ATTR_SUCCESS)) {
            log.error(getErrors(response.body().getJsonArray(ATTR_ERRORS)));
        }
    }

    private JsonObject syncLeads(JsonObject payload) {
        Response<JsonObject> response = leadClient.syncLeads(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload);
        if (response.status() == MarketoApiConstants.HTTP_STATUS_OK && response.body().getBoolean(ATTR_SUCCESS)) {
            handleListResponse(listClient.addToList(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, listId, listPayload));
        }
        // return main action status
        return handleResponse(response);
    }

}
