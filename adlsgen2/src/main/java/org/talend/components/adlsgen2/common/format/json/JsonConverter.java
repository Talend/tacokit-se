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

import java.util.Optional;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.talend.components.adlsgen2.common.converter.RecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;

@Slf4j
public class JsonConverter implements RecordConverter<JsonObject> {

    @Service
    public static RecordBuilderFactory recordBuilderFactory;

    public static JsonConverter of() {
        return new JsonConverter();
    }

    protected JsonConverter() {
    }

    @Override
    public Schema inferSchema(JsonObject record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        record.entrySet().stream().map(s -> createEntry(s.getKey(), s.getValue())).forEach(builder::withEntry);
        return builder.build();
        // return toRecord(record).getSchema();
    }

    /**
     *
     */

    private Entry createEntry(String name, JsonValue jsonValue) {
        log.warn("[createEntry#{}] ({}) {} ", name, jsonValue.getValueType(), jsonValue);

        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        builder.withName(name);
        Schema.Builder nestedSchemaBuilder;
        switch (jsonValue.getValueType()) {
        case ARRAY:
            builder.withType(Type.ARRAY);
            nestedSchemaBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
            populateJsonArrayEntries(nestedSchemaBuilder, jsonValue.asJsonArray());
            builder.withElementSchema(nestedSchemaBuilder.build());
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
        log.warn("[createEntry#{}] generated ({}) {} ", name, entry);
        return entry;
    }

    private Entry createEntry(JsonValue jsonValue) {
        log.warn("[createEntry] ({}) {} ", jsonValue.getValueType(), jsonValue);
        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        if (jsonValue.getValueType().equals(ValueType.ARRAY)) {
            Schema.Builder nestedSchemaBuilder;
            if (jsonValue.asJsonArray().get(0).getValueType().equals(ValueType.OBJECT)) {
                nestedSchemaBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
                // populateJsonArrayEntries(nestedSchemaBuilder, jsonValue.asJsonArray());
                populateJsonObjectEntries(nestedSchemaBuilder, jsonValue.asJsonObject());
            } else {
                nestedSchemaBuilder = recordBuilderFactory.newSchemaBuilder(Type.ARRAY);
                populateJsonArrayEntries(nestedSchemaBuilder, jsonValue.asJsonArray());
            }
            builder.withElementSchema(nestedSchemaBuilder.build());
        } else {
            builder.withType(translateType(jsonValue));
        }

        Entry entry = builder.build();
        log.warn("[createEntry] generated ({}) {} ", entry);
        return entry;
    }

    private void populateJsonArrayEntries(Schema.Builder builder, JsonArray value) {
        // either primitive
        // objects ?
        value.asJsonArray().stream().map(this::createEntry).forEach(builder::withEntry);
    }

    private void populateJsonObjectEntries(Schema.Builder builder, JsonObject value) {
        value.entrySet().stream().map(s -> createEntry(s.getKey(), s.getValue())).forEach(builder::withEntry);
    }

    @Override
    public Record toRecord(JsonObject record) {
        final Schema schema = inferSchema(record);
        return convertJsonObjectToRecord(schema, record);
    }

    @Override
    public JsonObject fromRecord(Record record) {
        throw new UnsupportedOperationException("#fromRecord()");
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
        throw new RuntimeException("The data type " + value.getValueType() + " is not handled.");
    }

    // private List<Record> convertJsonArrayToRecords(Schema schema, JsonArray jsonArray) {
    // if (jsonArray.get(0).getValueType().equals(ValueType.OBJECT)) {
    // return jsonArray.asJsonArray().stream().map(JsonValue::asJsonObject)
    // .map(json -> convertJsonObjectToRecord(schema, json)).collect(toList());
    // } else {
    // return jsonArray.asJsonArray().stream().map(json -> convertJsonValueToRecord(schema, json)).collect(toList());
    // }
    // }

    private Record convertJsonObjectToRecord(Schema schema, JsonObject json) {
        log.warn("[convertJsonObjectToRecord] ({}) {} {}", json.getValueType(), schema, json);
        final Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        schema.getEntries().stream().forEach(entry -> {
            log.warn("entry : {}", entry);
            switch (entry.getType()) {
            case RECORD:
                log.error("json.get(entry.getName() {}", json.get(entry.getName()));
                builder.withRecord(entry, Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v))
                        .map(value -> convertJsonObjectToRecord(entry.getElementSchema(), value.asJsonObject())).orElse(null));
                break;
            case ARRAY:
                log.error(" {}", entry.getElementSchema());
                log.error("ARRAY: [{} - {}] {}", json.getValueType(), entry.getElementSchema().getType(),
                        json.get(entry.getName()));
                switch (entry.getElementSchema().getType()) {
                case RECORD:
                    builder.withRecord(entry,
                            Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v))
                                    .map(value -> convertJsonObjectToRecord(entry.getElementSchema(), value.asJsonObject()))
                                    .orElse(null));
                    // builder.withArray(entry,
                    // convertJsonArrayToRecords(entry.getElementSchema(), json.getJsonArray(entry.getName())));
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
                    throw new RuntimeException("Test Record doesn't contain any other data types");
                }
                }
                // builder.withRecord(entry, Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v))
                // .map(value -> convertJsonObjectToRecord(entry.getElementSchema(), value.asJsonObject())).orElse(null));
                break;
            case STRING:
                builder.withString(entry, Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v))
                        .map(JsonString.class::cast).map(JsonString::getString).orElse(null));
                break;
            case LONG:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonNumber.class::cast)
                        .map(JsonNumber::longValue).ifPresent(value -> builder.withLong(entry, value));
                break;
            case DOUBLE:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonNumber.class::cast)
                        .map(JsonNumber::doubleValue).ifPresent(value -> builder.withDouble(entry, value));
                break;
            case BOOLEAN:
                Optional.ofNullable(json.get(entry.getName())).filter(v -> !JsonValue.NULL.equals(v)).map(JsonValue.TRUE::equals)
                        .ifPresent(value -> builder.withBoolean(entry, value));
                break;
            case INT:
            case FLOAT:
            case BYTES:
            case DATETIME:
                break;
            }

        });

        return builder.build();

    }

}
