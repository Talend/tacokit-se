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
package org.talend.components.adlsgen2.common.format.json;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Optional;

import javax.json.JsonBuilderFactory;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.talend.components.adlsgen2.common.converter.RecordConverter;
import org.talend.components.adlsgen2.common.format.FileFormatRuntimeException;
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

    private final JsonConfiguration configuration;

    private RecordBuilderFactory recordBuilderFactory;

    private JsonBuilderFactory jsonBuilderFactory;

    private Schema schema;

    private JsonObjectBuilder rcd;

    public static JsonConverter of(final RecordBuilderFactory factory, final JsonBuilderFactory jsonFactory,
            final @Configuration("jsonConfiguration") JsonConfiguration configuration) {
        return new JsonConverter(factory, jsonFactory, configuration);
    }

    protected JsonConverter(final RecordBuilderFactory factory, final JsonBuilderFactory jsonFactory,
            final @Configuration("jsonConfiguration") JsonConfiguration configuration) {
        this.recordBuilderFactory = factory;
        this.jsonBuilderFactory = jsonFactory;
        this.configuration = configuration;
    }

    @Override
    public Schema inferSchema(final JsonObject record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        record.entrySet().stream().map(s -> createEntry(s.getKey(), s.getValue())).forEach(builder::withEntry);
        return builder.build();
    }

    @Override
    public Record toRecord(final JsonObject record) {
        if (schema == null) {
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
            log.debug("[convertRecordToJsonObject] entry:{}; type: {}; value: {}.", entry.getName(), entry.getType(),
                    record.get(Object.class, entry.getName()));

            switch (entry.getType()) {
            case RECORD:
                Record subRecord = record.getRecord(entry.getName());
                json.add(entry.getName(), convertRecordToJsonObject(subRecord));
                break;
            case ARRAY:
                break;
            case STRING:
                json.add(entry.getName(), record.getString(entry.getName()));
                break;
            case BYTES:
                json.add(entry.getName(), record.getBytes(entry.getName()).toString());
                break;
            case INT:
                json.add(entry.getName(), record.getInt(entry.getName()));
                break;
            case LONG:
                json.add(entry.getName(), record.getLong(entry.getName()));
                break;
            case FLOAT:
                json.add(entry.getName(), record.getFloat(entry.getName()));
                break;
            case DOUBLE:
                json.add(entry.getName(), record.getDouble(entry.getName()));
                break;
            case BOOLEAN:
                json.add(entry.getName(), record.getBoolean(entry.getName()));
                break;
            case DATETIME:
                json.add(entry.getName(), record.getDateTime(entry.getName()).toString());
                break;
            }
        }
        return json.build();
    }

    private JsonObjectBuilder convertRecordToJsonObject(JsonObjectBuilder json, Entry entry, Record record) {
        log.warn("[convertRecordToJsonObject] entry:{}; type: {}; value: {}.", entry.getName(), entry.getType(),
                record.get(Object.class, entry.getName()));
        switch (entry.getType()) {
        case RECORD:
            JsonObjectBuilder rcd = jsonBuilderFactory.createObjectBuilder();
            Record subRecord = record.getRecord(entry.getName());
            for (Entry subEntry : entry.getElementSchema().getEntries()) {
                log.warn("[convertRecordToJsonObject] subentry:{}; type: {}; value: {}.", subEntry.getName(), subEntry.getType(),
                        subRecord.get(Object.class, subEntry.getName()));
                rcd.add(subEntry.getName(), convertRecordToJsonObject(rcd, subEntry, subRecord));
            }
            log.warn("[.] {}", rcd);
            json.add(entry.getName(), rcd);
            break;
        case ARRAY:
            break;
        case STRING:
            json.add(entry.getName(), record.getString(entry.getName()));
            break;
        case BYTES:
            json.add(entry.getName(), record.getBytes(entry.getName()).toString());
            break;
        case INT:
            json.add(entry.getName(), record.getInt(entry.getName()));
            break;
        case LONG:
            json.add(entry.getName(), record.getLong(entry.getName()));
            break;
        case FLOAT:
            json.add(entry.getName(), record.getFloat(entry.getName()));
            break;
        case DOUBLE:
            json.add(entry.getName(), record.getDouble(entry.getName()));
            break;
        case BOOLEAN:
            json.add(entry.getName(), record.getBoolean(entry.getName()));
            break;
        case DATETIME:
            json.add(entry.getName(), record.getDateTime(entry.getName()).toString());
            break;
        }
        return json;
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
            builder.withElementSchema(nestedSchemaBuilder.build());
            break;
        case STRING:
        case NUMBER:
        case TRUE:
        case FALSE:
        case NULL:
            builder.withType(translateType(jsonValue));
            break;
        }
        Entry entry = builder.build();
        log.debug("[createEntry#{}] generated ({}) {} ", name, entry);
        return entry;
    }

    private void populateJsonObjectEntries(Schema.Builder builder, JsonObject value) {
        value.entrySet().stream().map(s -> createEntry(s.getKey(), s.getValue())).forEach(builder::withEntry);
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
        throw new FileFormatRuntimeException("The data type " + value.getValueType() + " is not handled.");
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
                    throw new FileFormatRuntimeException("Test Record doesn't contain any other data types");
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
