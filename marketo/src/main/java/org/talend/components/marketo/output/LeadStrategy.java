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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LOOKUP_FIELD;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.OutputAction;
import org.talend.components.marketo.service.LeadClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.Toolbox;
import org.talend.sdk.component.api.configuration.Option;

public class LeadStrategy extends OutputComponentStrategy implements ProcessorStrategy {

    private LeadClient leadClient;

    private transient static final Logger LOG = LoggerFactory.getLogger(LeadStrategy.class);

    public LeadStrategy(@Option("configuration") final MarketoOutputDataSet dataSet, //
            final MarketoService service, //
            final Toolbox tools) {
        super(dataSet, service, tools);
        this.leadClient = service.getLeadClient();
        this.leadClient.base(this.dataSet.getDataStore().getEndpoint());
    }

    @Override
    public JsonObject getPayload(JsonObject incomingData) {
        JsonObject data = incomingData;
        JsonArray input = jsonFactory.createArrayBuilder().add(data).build();
        LOG.warn("[getPayload] data : {}", data);
        LOG.warn("[getPayload] input: {}", input);
        if (OutputAction.sync.equals(dataSet.getAction())) {
            return jsonFactory.createObjectBuilder() //
                    .add(ATTR_ACTION, dataSet.getSyncMethod().name()) //
                    .add(ATTR_LOOKUP_FIELD, dataSet.getLookupField()) //
                    .add(ATTR_INPUT, input) //
                    .build();
        } else {
            return jsonFactory.createObjectBuilder() //
                    .add(ATTR_INPUT, input) //
                    .build();
        }
    }

    @Override
    public JsonObject runAction(JsonObject payload) {
        switch (dataSet.getAction()) {
        case sync:
            return syncLeads(payload);
        case delete:
            return deleteLeads(payload);
        }
        throw new UnsupportedOperationException(i18n.invalidOperation());
    }

    private JsonObject deleteLeads(JsonObject payload) {
        return handleResponse(leadClient.deleteLeads(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
    }

    private JsonObject syncLeads(JsonObject payload) {
        return handleResponse(leadClient.syncLeads(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
    }

}
