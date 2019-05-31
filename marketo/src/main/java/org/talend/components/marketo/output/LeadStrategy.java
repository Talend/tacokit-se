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
import javax.json.JsonObject;

import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.OutputAction;
import org.talend.components.marketo.service.LeadClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.configuration.Option;

import lombok.extern.slf4j.Slf4j;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACTION;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LOOKUP_FIELD;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;

@Slf4j
public class LeadStrategy extends OutputComponentStrategy implements ProcessorStrategy {

    private LeadClient leadClient;

    public LeadStrategy(@Option("configuration") final MarketoOutputConfiguration dataSet, //
            final MarketoService service) {
        super(dataSet, service);
        this.leadClient = service.getLeadClient();
        this.leadClient.base(this.configuration.getDataSet().getDataStore().getEndpoint());
    }

    @Override
    public JsonObject getPayload(List<JsonObject> incomingData) {
        JsonArray input = jsonFactory.createArrayBuilder(incomingData).build();
        if (OutputAction.delete.equals(configuration.getAction())) {
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
        switch (configuration.getAction()) {
        case delete:
            return deleteLeads(payload);
        default:
            return syncLeads(payload);
        }
    }

    private JsonObject deleteLeads(JsonObject payload) {
        return handleResponse(leadClient.deleteLeads(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
    }

    private JsonObject syncLeads(JsonObject payload) {
        return handleResponse(leadClient.syncLeads(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
    }

}
