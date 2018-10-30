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
import javax.json.JsonObject;

import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.OutputAction;
import org.talend.components.marketo.service.CompanyClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.Toolbox;
import org.talend.sdk.component.api.configuration.Option;

public class CompanyStrategy extends OutputComponentStrategy implements ProcessorStrategy {

    private CompanyClient companyClient;

    public CompanyStrategy(@Option("configuration") final MarketoOutputDataSet dataSet, //
            final MarketoService service, //
            final Toolbox tools) {
        super(dataSet, service, tools);
        this.companyClient = service.getCompanyClient();
        this.companyClient.base(this.dataSet.getDataStore().getEndpoint());
    }

    @Override
    public JsonObject runAction(JsonObject payload) {
        switch (dataSet.getAction()) {
        case sync:
            return syncCompanies(payload);
        case delete:
            return deleteCompanies(payload);
        }
        throw new RuntimeException(i18n.invalidOperation());
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

    private JsonObject deleteCompanies(JsonObject payload) {
        return handleResponse(companyClient.deleteCompanies(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
    }

    private JsonObject syncCompanies(JsonObject payload) {
        return handleResponse(companyClient.syncCompanies(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
    }

}
