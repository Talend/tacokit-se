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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CREATED_AT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MARKETO_GUID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_REASONS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SEQ;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_STATUS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_UPDATED_AT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_WORKSPACE_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Schema.Entry;
import org.talend.sdk.component.api.service.schema.Type;

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


    public void initClients(MarketoDataStore dataStore){
        authorizationClient.base(dataStore.getEndpoint());
        leadClient.base(dataStore.getEndpoint());
        listClient.base(dataStore.getEndpoint());
        customObjectClient.base(dataStore.getEndpoint());
        companyClient.base(dataStore.getEndpoint());
        opportunityClient.base(dataStore.getEndpoint());
    }

    public String getFieldsFromDescribeFormatedForApi(JsonArray fields) {
        List<String> result = new ArrayList<>();
        for (JsonObject field : fields.getValuesAs(JsonObject.class)) {
            if (field.getJsonObject("rest") != null) {
                result.add(field.getJsonObject("rest").getString(ATTR_NAME));
            } else {
                result.add(field.getString(ATTR_NAME));
            }
        }
        return result.stream().collect(joining(","));
    }

    protected JsonArray parseResultFromResponse(Response<JsonObject> response) throws IllegalArgumentException {
        if (response.status() == 200 && response.body() != null && response.body().getJsonArray(ATTR_RESULT) != null) {
            return response.body().getJsonArray(ATTR_RESULT);
        }
        LOG.error("[parseResultFromResponse] Error: [{}] headers:{}; body: {}.", response.status(), response.headers(),
                response.body());
        throw new IllegalArgumentException(i18n.invalidOperation());
    }

    protected Schema getSchemaForEntity(JsonArray entitySchema) {
        Schema s = new Schema();
        Collection<Entry> entries = new ArrayList<>();
        for (JsonObject field : entitySchema.getValuesAs(JsonObject.class)) {
            Entry entry = new Entry();
            if (field.getJsonObject("rest") != null) {
                entry.setName(field.getJsonObject("rest").getString(ATTR_NAME));
            } else {
                entry.setName(field.getString(ATTR_NAME));
            }
            String dataType = field.getString("dataType", "string");
            switch (dataType) {
            case ("string"):
            case ("text"):
            case ("phone"):
            case ("email"):
            case ("url"):
            case ("lead_function"):
            case ("reference"):
                entry.setType(Type.STRING);
                break;
            case ("integer"):
                entry.setType(Type.INT);
                break;
            case ("boolean"):
                entry.setType(Type.BOOLEAN);
                break;
            case ("float"):
            case ("currency"):
                entry.setType(Type.DOUBLE);
                break;
            case ("date"):
            case ("datetime"):
                entry.setType(Type.STRING);
                break;
            default:
                LOG.warn("Non managed type : {}. for {}. Defaulting to String.", dataType, this);
                entry.setType(Type.STRING);
            }
            entries.add(entry);
        }
        s.setEntries(entries);
        return s;
    }

    public Schema getOutputSchema(MarketoEntity entity) {
        Schema schema = new Schema();
        Collection<Entry> entries = new ArrayList<>();
        switch (entity) {
        case Lead:
        case List:
            entries.add(new Entry(ATTR_ID, Type.INT));
            entries.add(new Entry(ATTR_STATUS, Type.STRING));
            entries.add(new Entry(ATTR_REASONS, Type.STRING));
            break;
        case CustomObject:
            entries.add(new Entry(ATTR_SEQ, Type.INT));
            entries.add(new Entry(ATTR_MARKETO_GUID, Type.INT));
            entries.add(new Entry(ATTR_STATUS, Type.STRING));
            entries.add(new Entry(ATTR_REASONS, Type.STRING));
            break;
        case Company:
        case Opportunity:
        case OpportunityRole:
            entries.add(new Entry(ATTR_SEQ, Type.INT));
            entries.add(new Entry(ATTR_MARKETO_GUID, Type.STRING));
            entries.add(new Entry(ATTR_STATUS, Type.STRING));
            entries.add(new Entry(ATTR_REASONS, Type.STRING));
            break;
        }
        schema.setEntries(entries);
        return schema;
    }

    // TODO this is not the correct defaults schemas!!!
    public Schema getInputSchema(MarketoEntity entity, String action) {
        Schema schema = new Schema();
        Collection<Entry> entries = new ArrayList<>();
        switch (entity) {
        case Lead:
        case List:
            switch (action) {
            case "isMemberOfList":
                entries.add(new Entry(ATTR_ID, Type.INT));
                entries.add(new Entry(ATTR_STATUS, Type.STRING));
                entries.add(new Entry(ATTR_REASONS, Type.STRING));
                break;
            case "list":
            case "get":
                entries.add(new Entry(ATTR_ID, Type.INT));
                entries.add(new Entry(ATTR_NAME, Type.STRING));
                entries.add(new Entry(ATTR_WORKSPACE_NAME, Type.STRING));
                entries.add(new Entry(ATTR_CREATED_AT, Type.STRING));
                entries.add(new Entry(ATTR_UPDATED_AT, Type.STRING));
                break;
            default:
                entries.add(new Entry(ATTR_ID, Type.INT));
                entries.add(new Entry(ATTR_STATUS, Type.STRING));
                entries.add(new Entry(ATTR_REASONS, Type.STRING));
            }
            break;
        case CustomObject:
        case Company:
        case Opportunity:
        case OpportunityRole:
            entries.add(new Entry(ATTR_SEQ, Type.INT));
            entries.add(new Entry(ATTR_MARKETO_GUID, Type.STRING));
            entries.add(new Entry(ATTR_STATUS, Type.STRING));
            entries.add(new Entry(ATTR_REASONS, Type.STRING));
            break;
        }
        schema.setEntries(entries);
        return schema;
    }

}
