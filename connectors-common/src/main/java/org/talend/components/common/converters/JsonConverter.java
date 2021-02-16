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
package org.talend.components.common.converters;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Optional;

import javax.json.JsonBuilderFactory;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.talend.components.common.formats.JSONFormatOptions;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;
import static java.util.stream.Collectors.toList;

@Slf4j
public class JsonConverter implements RecordConverter<JsonObject>, Serializable {

    private final JSONFormatOptions configuration;

    private RecordBuilderFactory recordBuilderFactory;

    private JsonBuilderFactory jsonBuilderFactory;

    private Schema schema;

    public static JsonConverter of(final RecordBuilderFactory factory, final JsonBuilderFactory jsonFactory,
            final @Configuration("jsonConfiguration") JSONFormatOptions configuration) {
        return new JsonConverter(factory, jsonFactory, configuration);
    }

    protected JsonConverter(final RecordBuilderFactory factory, final JsonBuilderFactory jsonFactory,
            final @Configuration("jsonConfiguration") JSONFormatOptions configuration) {
        this.recordBuilderFactory = factory;
        this.jsonBuilderFactory = jsonFactory;
        this.configuration = configuration;
    }

    @Override
    public Schema inferSchema(final JsonObject record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        populateJsonObjectEntries(builder, record);
        return builder.build();
    }

    @Override
    public Record toRecord(final JsonObject record) {
        if (schema == null || record.keySet().size() > schema.getEntries().size()) {
            schema = inferSchema(record);
        }
        return convertJsonObjectToRecord(schema, record);
    }

    @Override
    public JsonObject fromRecord(final Record record) {
        return convertRecordToJsonObject(record);
    }

    private JsonObject convertRecordToJsonObject(Record record) {
        JsonObjectBuilder json = jsonBuilderFactory.createObjectBuilder();
        for (Entry entry : record.getSchema().getEntries()) {
            String fieldName = entry.getName();
            Object val = record.get(Object.class, fieldName);
            log.debug("[convertRecordToJsonObject] entry: {}; type: {}; value: {}.", fieldName, entry.getType(), val);
            if (null == val) {
                json.add(fieldName, JsonObject.NULL);
                continue;
            }
            switch (entry.getType()) {
            case RECORD:
                Record subRecord = record.getRecord(fieldName);
                json.add(fieldName, convertRecordToJsonObject(subRecord));
                break;
            case ARRAY:
                break;
            case STRING:
                json.add(fieldName, record.getString(fieldName));
                break;
            case BYTES:
                json.add(fieldName, record.getBytes(fieldName).toString());
                break;
            case INT:
                json.add(fieldName, record.getInt(fieldName));
                break;
            case LONG:
                json.add(fieldName, record.getLong(fieldName));
                break;
            case FLOAT:
                json.add(fieldName, record.getFloat(fieldName));
                break;
            case DOUBLE:
                json.add(fieldName, record.getDouble(fieldName));
                break;
            case BOOLEAN:
                json.add(fieldName, record.getBoolean(fieldName));
                break;
            case DATETIME:
                json.add(fieldName, record.getDateTime(fieldName).toString());
                break;
            }
        }
        return json.build();
    }

    /**
     *
     */
    private Entry createEntry(String name, JsonValue jsonValue) {
        log.debug("[createEntry#{}] ({}) {} ", name, jsonValue.getValueType(), jsonValue);
        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        builder.withName(name);
        Schema.Builder nestedSchemaBuilder;
        switch (jsonValue.getValueType()) {
        case ARRAY:
            JsonValue jv = jsonValue.asJsonArray().get(0);
            Type tt = translateType(jsonValue.asJsonArray().get(0));
            nestedSchemaBuilder = recordBuilderFactory.newSchemaBuilder(tt);
            switch (tt) {
            case RECORD:
                builder.withType(Type.RECORD);
                populateJsonObjectEntries(nestedSchemaBuilder, jv.asJsonObject());
                break;
            case ARRAY:
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case DATETIME:
                break;
            }
            builder.withElementSchema(nestedSchemaBuilder.build()).withType(Type.ARRAY).withNullable(true);
            break;
        case OBJECT:
            builder.withType(Type.RECORD);
            nestedSchemaBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
            populateJsonObjectEntries(nestedSchemaBuilder, jsonValue.asJsonObject());
            builder.withElementSchema(nestedSchemaBuilder.build()).withNullable(true);
            break;
        case STRING:
        case NUMBER:
        case TRUE:
        case FALSE:
        case NULL:
            builder.withType(translateType(jsonValue)).withNullable(true);
            break;
        }
        Entry entry = builder.build();
        log.debug("[createEntry#{}] generated ({}) {} ", name, entry);
        return entry;
    }

    private void populateJsonObjectEntries(Schema.Builder builder, JsonObject value) {
        value.entrySet().stream().filter(e -> e.getValue() != javax.json.JsonValue.NULL)
                .map(s -> createEntry(s.getKey(), s.getValue())).forEach(builder::withEntry);
    }

    public Type translateType(JsonValue value) {
        switch (value.getValueType()) {
        case STRING:
            return Type.STRING;
        case NUMBER:
            return ((JsonNumber) value).isIntegral() ? Type.LONG : Type.DOUBLE;
        case TRUE:
        case FALSE:
            return Type.BOOLEAN;
        case ARRAY:
            return Type.ARRAY;
        case OBJECT:
            return Type.RECORD;
        case NULL:
            break;
        }
        throw new IllegalStateException("The data type " + value.getValueType() + " is not handled.");
    }

    private Record convertJsonObjectToRecord(Schema schema, JsonObject json) {
        final Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        schema.getEntries().stream().forEach(entry -> {
            switch (entry.getType()) {
            case RECORD:
                builder.withRecord(entry, Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v))
                        .map(value -> convertJsonObjectToRecord(entry.getElementSchema(), value.asJsonObject())).orElse(null));
                break;
            case ARRAY:
                switch (entry.getElementSchema().getType()) {
                case RECORD:
                    builder.withArray(entry, json.getJsonArray(entry.getName()).stream()
                            .map(v -> convertJsonObjectToRecord(entry.getElementSchema(), v.asJsonObject())).collect(toList()));
                    break;
                case ARRAY:
                    log.error("[convertJsonObjectToRecord] Not supporting array of array: {}",
                            json.get(0).asJsonArray().get(0).getValueType());
                    break;
                case STRING:
                    builder.withArray(entry, json.getJsonArray(entry.getName()).stream().map(JsonString.class::cast)
                            .map(JsonString::getString).collect(toList()));
                    break;
                case LONG:
                    builder.withArray(entry, json.getJsonArray(entry.getName()).stream().map(JsonNumber.class::cast)
                            .map(JsonNumber::longValue).collect(toList()));
                    break;
                case DOUBLE:
                    builder.withArray(entry, json.getJsonArray(entry.getName()).stream().map(JsonNumber.class::cast)
                            .map(JsonNumber::doubleValue).collect(toList()));
                    break;
                case BOOLEAN:
                    builder.withArray(entry,
                            json.getJsonArray(entry.getName()).stream().map(JsonValue.TRUE::equals).collect(toList()));
                    break;
                default: {
                    throw new IllegalStateException("Test Record doesn't contain any other data types");
                }
                }
                break;
            case STRING:
                builder.withString(entry, Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v))
                        .map(JsonString.class::cast).map(JsonString::getString).orElse(null));
                break;
            case INT:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonNumber.class::cast)
                        .map(JsonNumber::intValue).ifPresent(value -> builder.withInt(entry, value));
                break;
            case LONG:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonNumber.class::cast)
                        .map(JsonNumber::longValue).ifPresent(value -> builder.withLong(entry, value));
                break;
            case FLOAT:
            case DOUBLE:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonNumber.class::cast)
                        .map(JsonNumber::doubleValue).ifPresent(value -> builder.withDouble(entry, value));
                break;
            case BOOLEAN:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonValue.TRUE::equals)
                        .ifPresent(value -> builder.withBoolean(entry, value));
                break;
            case BYTES:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonNumber.class::cast)
                        .map(JsonString.class::cast).ifPresent(value -> builder.withBytes(entry, value.toString().getBytes()));
                break;
            case DATETIME:
                try {
                    Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v))
                            .map(JsonNumber.class::cast).map(JsonString.class::cast)
                            .ifPresent(value -> builder.withDateTime(entry, ZonedDateTime.parse(value.toString())));
                } catch (Exception e) {
                    log.error("[convertJsonObjectToRecord] parse ZonedDateTime failed for {} : {}.", entry.getName(),
                            json.get(entry.getName()));
                }
                break;
            }
        });

        return builder.build();
    }

}
