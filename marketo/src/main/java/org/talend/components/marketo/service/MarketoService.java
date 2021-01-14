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
package org.talend.components.marketo.service;

import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import javax.json.JsonWriterFactory;

import org.talend.components.marketo.MarketoRuntimeException;
import org.talend.components.marketo.dataset.MarketoDataSet;
import org.talend.components.marketo.dataset.MarketoInputConfiguration;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Builder;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACCESS_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACTIVITY_DATE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACTIVITY_TYPE_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ATTRIBUTES;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CAMPAIGN_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CODE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CREATED_AT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ERRORS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MARKETO_GUID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_MESSAGE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_PRIMARY_ATTRIBUTE_VALUE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_PRIMARY_ATTRIBUTE_VALUE_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_REASONS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SEQ;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_STATUS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_SUCCESS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_UPDATED_AT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_WORKSPACE_NAME;
import static org.talend.components.marketo.MarketoApiConstants.HTTP_STATUS_OK;
import static org.talend.components.marketo.service.AuthorizationClient.CLIENT_CREDENTIALS;

@Accessors
@Slf4j
@Service
public class MarketoService {

    protected static final String DATETIME = "datetime";

    @Getter
    @Service
    protected I18nMessage i18n;

    @Getter
    @Service
    protected RecordBuilderFactory recordBuilder;

    @Getter
    @Service
    private JsonBuilderFactory jsonFactory;

    @Getter
    @Service
    private JsonReaderFactory jsonReader;

    @Getter
    @Service
    private JsonWriterFactory jsonWriter;

    @Getter
    @Service
    protected AuthorizationClient authorizationClient;

    @Getter
    @Service
    protected LeadClient leadClient;

    @Getter
    @Service
    protected CustomObjectClient customObjectClient;

    @Getter
    @Service
    protected CompanyClient companyClient;

    @Getter
    @Service
    protected OpportunityClient opportunityClient;

    @Getter
    @Service
    protected ListClient listClient;

    public void initClients(MarketoDataStore dataStore) {
        authorizationClient.base(dataStore.getEndpoint());
        leadClient.base(dataStore.getEndpoint());
        listClient.base(dataStore.getEndpoint());
        customObjectClient.base(dataStore.getEndpoint());
        companyClient.base(dataStore.getEndpoint());
        opportunityClient.base(dataStore.getEndpoint());
    }

    /**
     * Retrieve an set an access token for using API
     */
    public String retrieveAccessToken(@Configuration("configuration") final MarketoDataSet dataSet) {
        initClients(dataSet.getDataStore());
        Response<JsonObject> result = authorizationClient.getAuthorizationToken(CLIENT_CREDENTIALS,
                dataSet.getDataStore().getClientId(), dataSet.getDataStore().getClientSecret());
        log.debug("[retrieveAccessToken] [{}] : {}.", result.status(), result.body());
        if (result.status() == 200) {
            return result.body().getString(ATTR_ACCESS_TOKEN);
        } else {
            String error = i18n.accessTokenRetrievalError(result.status(), result.headers().toString());
            log.error("[retrieveAccessToken] {}", error);
            throw new MarketoRuntimeException(error);
        }
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

    protected JsonArray parseResultFromResponse(Response<JsonObject> response) {
        if (response.status() == 200 && response.body() != null && response.body().getJsonArray(ATTR_RESULT) != null) {
            return response.body().getJsonArray(ATTR_RESULT);
        }
        log.error("[parseResultFromResponse] Error: [{}] headers:{}; body: {}.", response.status(), response.headers(),
                response.body());
        throw new IllegalArgumentException(i18n.invalidOperation());
    }

    public Schema getEntitySchema(final MarketoInputConfiguration configuration) {
        Schema s = null;
        switch (configuration.getDataSet().getLeadAction()) {
        case getLeadsByList:
            Builder b = recordBuilder.newSchemaBuilder(Type.RECORD);
            List<String> fields = configuration.getDataSet().getFields();
            Schema people = getEntitySchema(configuration.getDataSet().getDataStore());
            if (fields.size() > 0) {
                people.getEntries().stream().filter(entry -> fields.contains(entry.getName()))
                        .forEach(entry -> b.withEntry(entry));
            } else {
                people.getEntries().stream().forEach(entry -> b.withEntry(entry));
            }
            s = b.build();
            break;
        case getLeadActivity:
            s = getLeadActivitiesSchema();
            break;
        }
        log.warn("[getEntitySchema] schema: {}", s.getEntries().stream().map(Entry::getName).collect(toList()));
        return s;
    }

    public Schema getEntitySchema(final MarketoDataStore dataStore) {
        try {
            initClients(dataStore);
            String accessToken = authorizationClient.getAccessToken(dataStore);
            JsonArray entitySchema = null;
            entitySchema = parseResultFromResponse(leadClient.describeLead(accessToken));
            return getSchemaForEntity(entitySchema);
        } catch (Exception e) {
            log.error(i18n.exceptionOccured(e.getMessage()));
        }
        return null;
    }

    private Schema mergeSchemas(Schema first, Schema second) {
        Builder b = recordBuilder.newSchemaBuilder(Type.RECORD);
        first.getEntries().forEach(b::withEntry);
        second.getEntries().forEach(b::withEntry);
        return b.build();
    }

    protected Schema getSchemaForEntity(JsonArray entitySchema) {
        List<Entry> entries = new ArrayList<>();
        for (JsonObject field : entitySchema.getValuesAs(JsonObject.class)) {
            String entryName;
            Schema.Type entryType;
            String entityComment;
            if (field.getJsonObject("rest") != null) {
                entryName = field.getJsonObject("rest").getString(ATTR_NAME);
            } else {
                entryName = field.getString(ATTR_NAME);
            }
            String dataType = field.getString("dataType", "string");
            entityComment = dataType;
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
            case ("percent"):
            case ("score"):
            case ("integer"):
                entryType = Schema.Type.INT;
                break;
            case ("checkbox"):
            case ("boolean"):
                entryType = Schema.Type.BOOLEAN;
                break;
            case ("float"):
            case ("currency"):
                entryType = Type.FLOAT;
                break;
            case ("date"):
                /*
                 * Used for date. Follows W3C format. 2010-05-07
                 */
            case DATETIME:
                /*
                 * Used for a date & time. Follows W3C format (ISO 8601). The best practice is to always include the time zone
                 * offset.
                 * Complete date plus hours and minutes:
                 *
                 * YYYY-MM-DDThh:mmTZD
                 *
                 * where TZD is “+hh:mm” or “-hh:mm”
                 */
                entryType = Type.DATETIME;
                break;
            default:
                log.warn(i18n.nonManagedType(dataType, entryName));
                entryType = Schema.Type.STRING;
            }
            entries.add(
                    recordBuilder.newEntryBuilder().withName(entryName).withType(entryType).withComment(entityComment).build());
        }
        Builder b = recordBuilder.newSchemaBuilder(Schema.Type.RECORD);
        entries.forEach(b::withEntry);
        return b.build();
    }

    public JsonObject toJson(final Record record) {
        String recordStr = record.toString().replaceAll("AvroRecord\\{delegate=(.*)\\}$", "$1");
        JsonReader reader = jsonReader.createReader(new StringReader(recordStr));
        Throwable throwable = null;
        JsonObject json;
        try {
            json = reader.readObject();
        } catch (Throwable throwable1) {
            throwable = throwable1;
            throw throwable1;
        } finally {
            if (reader != null) {
                if (throwable != null) {
                    try {
                        reader.close();
                    } catch (Throwable throwable2) {
                        throwable.addSuppressed(throwable2);
                    }
                } else {
                    reader.close();
                }
            }
        }
        return json;
    }

    private boolean hasJsonValue(JsonValue value) {
        if (value == null) {
            return false;
        }
        if (value.getValueType().equals(ValueType.NULL)) {
            return false;
        }
        return true;
    }

    public Record convertToRecord(final JsonObject json, final Map<String, Entry> schema) {
        Record.Builder b = getRecordBuilder().newRecordBuilder();
        log.debug("[convertToRecord] json {} VS schema {}", json.entrySet().size(), schema.keySet().size());
        for (Entry entry : schema.values()) {
            String key = entry.getName();
            JsonValue val = json.get(key);
            switch (entry.getType()) {
            case ARRAY:
                String ary = "";
                if (val != null) {
                    json.getJsonArray(key).stream().map(JsonValue::toString).collect(joining(","));
                    // not in a sub array
                    if (!ary.contains("{")) {
                        ary = ary.replaceAll("\"", "").replaceAll("(\\[|\\])", "");
                    }
                }
                b.withString(key, ary);
                break;
            case RECORD:
            case BYTES:
            case STRING:
                if (hasJsonValue(val)) {
                    switch (val.getValueType()) {
                    case ARRAY:
                        b.withString(key, json.getJsonArray(key).stream().map(JsonValue::toString).collect(joining(",")));
                        break;
                    case OBJECT:
                        b.withString(key, String.valueOf(json.getJsonObject(key).toString()));
                        break;
                    case STRING:
                        b.withString(key, json.getString(key));
                        break;
                    case NUMBER:
                        b.withString(key, String.valueOf(json.getJsonNumber(key)));
                        break;
                    case TRUE:
                    case FALSE:
                        b.withString(key, String.valueOf(json.getBoolean(key)));
                        break;
                    case NULL:
                        b.withString(key, null);
                        break;
                    }
                } else {
                    b.withString(key, null);
                }
                break;
            case INT:
                b.withInt(key, hasJsonValue(val) ? json.getInt(key) : 0);
                break;
            case LONG:
                b.withLong(key, hasJsonValue(val) ? json.getJsonNumber(key).longValue() : 0);
                break;
            case FLOAT:
            case DOUBLE:
                b.withDouble(key, hasJsonValue(val) ? json.getJsonNumber(key).doubleValue() : 0);
                break;
            case BOOLEAN:
                b.withBoolean(key, hasJsonValue(val) ? json.getBoolean(key) : null);
                break;
            case DATETIME:
                try {
                    b.withDateTime(key,
                            hasJsonValue(val) ? new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(json.getString(key))
                                    : null);
                } catch (ParseException e1) {
                    log.error("[convertToRecord] Date parsing error: {}.", e1.getMessage());
                }
                break;
            }
        }
        Record record = b.build();
        log.debug("[convertToRecord] returning : {}.", record);
        return record;
    }

    /**
     * Convert Marketo Errors array to a single String (generally for Exception throwing).
     *
     * @param errors
     * @return flattened string
     */
    public String getErrors(JsonArray errors) {
        StringBuffer error = new StringBuffer();
        for (JsonObject json : errors.getValuesAs(JsonObject.class)) {
            error.append(String.format("[%s] %s", json.getString(ATTR_CODE), json.getString(ATTR_MESSAGE)));
        }

        return error.toString();
    }

    /**
     * Handle a typical Marketo response's payload to API call.
     *
     * @param response the http response
     * @return Marketo API result
     */
    public JsonObject handleResponse(final Response<JsonObject> response) {
        log.debug("[handleResponse] [{}] body: {}.", response.status(), response.body());
        if (response.status() == HTTP_STATUS_OK) {
            if (response.body().getBoolean(ATTR_SUCCESS)) {
                return response.body();
            } else {
                throw new MarketoRuntimeException(getErrors(response.body().getJsonArray(ATTR_ERRORS)));
            }
        }
        throw new MarketoRuntimeException(response.error(String.class));
    }

    Schema getLeadActivitiesSchema() {
        return recordBuilder.newSchemaBuilder(Type.RECORD)
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ACTIVITY_DATE).withType(Type.STRING)
                        .withComment(DATETIME).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ACTIVITY_TYPE_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ATTRIBUTES).withType(Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_CAMPAIGN_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_LEAD_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_MARKETO_GUID).withType(Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_PRIMARY_ATTRIBUTE_VALUE).withType(Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_PRIMARY_ATTRIBUTE_VALUE_ID).withType(Type.INT).build()) //
                .build();
    }

    Schema getLeadChangesSchema() {
        return recordBuilder.newSchemaBuilder(Type.RECORD)
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ACTIVITY_DATE).withType(Type.STRING)
                        .withComment(DATETIME).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ACTIVITY_TYPE_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ATTRIBUTES).withType(Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_CAMPAIGN_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_FIELDS).withType(Type.STRING).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_LEAD_ID).withType(Type.INT).build())
                .withEntry(recordBuilder.newEntryBuilder().withName(ATTR_MARKETO_GUID).withType(Type.STRING).build()) //
                .build();
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
