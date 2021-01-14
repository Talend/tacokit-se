/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.marketo.output;

import java.util.List;
import java.util.stream.Collectors;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;

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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_REASONS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SUCCESS;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;
import static org.talend.components.marketo.dataset.MarketoOutputConfiguration.OutputAction.delete;

@Slf4j
public class LeadStrategy extends OutputComponentStrategy implements ProcessorStrategy {

    private final ListClient listClient;

    private final LeadClient leadClient;

    private final String listId;

    public LeadStrategy(@Option("configuration") final MarketoOutputConfiguration dataSet, //
            final MarketoService service) {
        super(dataSet, service);
        leadClient = service.getLeadClient();
        listClient = service.getListClient();
        leadClient.base(configuration.getDataSet().getDataStore().getEndpoint());
        listClient.base(configuration.getDataSet().getDataStore().getEndpoint());
        listId = configuration.getDataSet().getListId();
    }

    @Override
    public JsonObject getPayload(List<JsonObject> incomingData) {
        JsonArray input = jsonFactory.createArrayBuilder(incomingData).build();
        if (delete.equals(configuration.getAction())) {
            return jsonFactory.createObjectBuilder() //
                    .add(ATTR_INPUT, input) //
                    .build();
        } else {
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

    private void addLeadsInList(List<JsonObject> leads) {
        JsonArrayBuilder builder = jsonFactory.createArrayBuilder();
        for (JsonObject lead : leads) {
            builder.add(jsonFactory.createObjectBuilder().add(ATTR_ID, lead.getInt(ATTR_ID)));
        }
        JsonObject listPayload = jsonFactory.createObjectBuilder().add(ATTR_INPUT, builder.build()).build();
        handleListResponse(listClient.addToList(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, listId, listPayload));
    }

    private void handleListResponse(Response<JsonObject> response) {
        if (response.status() == MarketoApiConstants.HTTP_STATUS_OK) {
            if (!response.body().getBoolean(ATTR_SUCCESS)) {
                log.error("[handleListResponse] Error during adding leads to list {}: {}", listId,
                        getErrors(response.body().getJsonArray(ATTR_ERRORS)));
            } else {
                response.body().getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class).stream().filter(this::isRejected)
                        .forEach(e -> log.error("[handleListResponse] Lead add to List {}: {}", listId,
                                getErrors(e.getJsonArray(ATTR_REASONS))));
            }
        }
    }

    private JsonObject syncLeads(JsonObject payload) {
        Response<JsonObject> response = leadClient.syncLeads(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload);
        if (response.status() == MarketoApiConstants.HTTP_STATUS_OK && response.body().getBoolean(ATTR_SUCCESS)) {
            addLeadsInList(response.body().getJsonArray(ATTR_RESULT).stream().map(JsonValue::asJsonObject)
                    .filter(lead -> !isRejected(lead)).collect(Collectors.toList()));
        }
        // return main action status
        return handleResponse(response);
    }

}
