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
package org.talend.components.marketo.service;

import static java.util.stream.Collectors.joining;
import static org.slf4j.LoggerFactory.getLogger;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;

import java.util.ArrayList;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;

@Service
public class MarketoService {

    @Service
    protected AuthorizationClient authorizationClient;

    @Service
    protected LeadClient leadClient;

    @Service
    protected CustomObjectClient customObjectClient;

    @Service
    protected CompanyClient companyClient;

    @Service
    protected OpportunityClient opportunityClient;

    @Service
    protected ListClient listClient;

    @Service
    protected I18nMessage i18n;

    private transient static final Logger LOG = getLogger(MarketoService.class);

    public String getFieldsFromDescribeFormatedForApi(JsonArray fields) {
        List<String> result = new ArrayList<>();
        for (JsonObject field : fields.getValuesAs(JsonObject.class)) {
            if (field.getJsonObject("rest") != null) {
                result.add(field.getJsonObject("rest").getString("name"));
            } else {
                result.add(field.getString("name"));
            }
        }
        return result.stream().collect(joining(","));
    }

    protected JsonArray parseResultFromResponse(Response<JsonObject> response) throws IllegalArgumentException {
        if (response.status() == 200 && response.body() != null && response.body().getJsonArray(ATTR_RESULT) != null) {
            return response.body().getJsonArray(ATTR_RESULT);
        }
        throw new IllegalArgumentException(i18n.invalidOperation());
    }

}
