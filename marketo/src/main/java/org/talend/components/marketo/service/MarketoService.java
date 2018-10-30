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
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Builder;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Service
public class MarketoService {

    @Service
    protected RecordBuilderFactory recordBuilder;

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

    public void initClients(MarketoDataStore dataStore) {
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
        List<Entry> entries = new ArrayList<>();
        for (JsonObject field : entitySchema.getValuesAs(JsonObject.class)) {
            String entryName;
            Schema.Type entryType;
            if (field.getJsonObject("rest") != null) {
                entryName = field.getJsonObject("rest").getString(ATTR_NAME);
            } else {
                entryName = field.getString(ATTR_NAME);
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
                entryType = Schema.Type.STRING;
                break;
            case ("integer"):
                entryType = Schema.Type.INT;
                break;
            case ("boolean"):
                entryType = Schema.Type.BOOLEAN;
                break;
            case ("float"):
            case ("currency"):
                entryType = Schema.Type.DOUBLE;
                break;
            case ("date"):
            case ("datetime"):
                entryType = Schema.Type.STRING;
                break;
            default:
                LOG.warn("Non managed type : {}. for {}. Defaulting to String.", dataType, this);
                entryType = Schema.Type.STRING;
            }
            entries.add(recordBuilder.newEntryBuilder().withName(entryName).withType(entryType).build());
        }
        Builder b = recordBuilder.newSchemaBuilder(Schema.Type.RECORD);
        entries.forEach(b::withEntry);
        return b.build();
    }

    public Schema getOutputSchema(MarketoEntity entity) {
        switch (entity) {
        case Lead:
        case List:
            return getLeadListDefaultSchema();
        case CustomObject:
        case Company:
        case Opportunity:
        case OpportunityRole:
            return getCustomObjectDefaultSchema();
        }
        return null;
    }

    // TODO this is not the correct defaults schemas!!!
    public Schema getInputSchema(MarketoEntity entity, String action) {
        switch (entity) {
        case Lead:
        case List:
            switch (action) {
            case "isMemberOfList":
                return getLeadListDefaultSchema();
            case "list":
            case "get":
                return getListGetDefaultSchema();
            default:
                return getLeadListDefaultSchema();
            }
        case CustomObject:
        case Company:
        case Opportunity:
        case OpportunityRole:
            return getCustomObjectDefaultSchema();
        }
        return null;
    }

    Schema getLeadListDefaultSchema() {
        return recordBuilder.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ID).withType(Schema.Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_STATUS).withType(Schema.Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_REASONS).withType(Schema.Type.STRING).build()).build();
    }

    Schema getListGetDefaultSchema() {
        return recordBuilder.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ID).withType(Schema.Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_NAME).withType(Schema.Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_WORKSPACE_NAME).withType(Schema.Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_CREATED_AT).withType(Schema.Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_UPDATED_AT).withType(Schema.Type.STRING).build())
                .build();
    }

    Schema getCustomObjectDefaultSchema() {
        return recordBuilder.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_SEQ).withType(Schema.Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_MARKETO_GUID).withType(Schema.Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_STATUS).withType(Schema.Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_REASONS).withType(Schema.Type.STRING).build()).build();
    }

}
