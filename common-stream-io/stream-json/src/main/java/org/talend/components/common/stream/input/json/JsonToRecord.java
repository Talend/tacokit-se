/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream.input.json;

import java.time.DateTimeException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * Translate json object to record.
 *
 * CAUTIONS :
 * records don't support variables objects in array ..
 * while in json, array can contains different kind of type :
 * `[ "Is a String", 123, { "field": "value" }, [1, 2, "text"] ]`
 * records does not.
 */
@Slf4j
public class JsonToRecord {

    /** record facotry */
    private final RecordBuilderFactory recordBuilderFactory;

    public JsonToRecord(RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
    }

    /**
     * Guess schema from json object.
     * 
     * @param json : json object.
     * @return guess record schema.
     */
    public Schema inferSchema(final JsonObject json) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        populateJsonObjectEntries(builder, json);
        return builder.build();
    }

    /**
     * Convert json object to record (with guessing schema).
     * 
     * @param json : json data.
     * @return data in record format.
     */
    public Record toRecord(final JsonObject json) {
        if (json == null) {
            return null;
        }

        final Schema schema = inferSchema(json);
        return convertJsonObjectToRecord(schema, json);
    }

    private Schema inferSchema(final JsonArray array) {
        if (array == null || array.isEmpty()) {
            return null;
        }
        final JsonValue value = array.get(0);
        if (value == null) {
            return null;
        }
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.ARRAY);
        final Schema subSchema;
        if (value.getValueType() == ValueType.OBJECT) {
            // if first element is object, supposing all elements are object (otherwise not compatible with record),
            // merge all object schemas.
            final JsonObject object = mergeAll(array);
            subSchema = inferSchema(object);
        } else if (value.getValueType() == ValueType.ARRAY) {
            subSchema = inferSchema(value.asJsonArray());
        } else {
            final Type type = translateType(value);
            subSchema = recordBuilderFactory.newSchemaBuilder(type).build();
        }
        builder.withElementSchema(subSchema);
        return builder.build();
    }

    /**
     * create a merged object for records.
     * allow array of differents jsonObject.
     * [ { "f1": "v1"}, {"f1":"v11", "f2": "V2"} ]
     */
    private JsonObject mergeAll(JsonArray array) {
        final JsonObjectBuilder objectBuilder = Json.createObjectBuilder();

        array.stream().filter((JsonValue v) -> v instanceof JsonObject).map(JsonValue::asJsonObject)
                .map((JsonObject j) -> Json.createObjectBuilder(j)).forEach(objectBuilder::addAll);
        return objectBuilder.build();
    }

    private void populateJsonObjectEntries(Schema.Builder builder, JsonObject value) {
        value.entrySet().stream().filter(e -> e.getValue() != JsonValue.NULL).map(s -> createEntry(s.getKey(), s.getValue()))
                .forEach(builder::withEntry);
    }

    private Entry createEntry(String name, JsonValue jsonValue) {
        log.debug("[createEntry#{}] ({}) {} ", name, jsonValue.getValueType(), jsonValue);
        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        // use comment to store the real element name, for example, "$oid"
        builder.withName(name).withNullable(true).withComment(name);

        switch (jsonValue.getValueType()) {
        case ARRAY:
            final Schema subSchema = this.inferSchema(jsonValue.asJsonArray());
            if (subSchema != null) {
                builder.withElementSchema(subSchema).withType(Type.ARRAY);
            }

            break;
        case OBJECT:
            builder.withType(Type.RECORD);
            Schema.Builder nestedSchemaBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
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
        log.debug("[createEntry#{}] generated ({})", name, entry);
        return entry;
    }

    private Record convertJsonObjectToRecord(Schema schema, JsonObject json) {
        final Record.Builder builder = recordBuilderFactory.newRecordBuilder(schema);
        schema.getEntries().stream().forEach((Entry entry) -> this.integrateEntryToRecord(entry, builder, json));

        return builder.build();
    }

    private String getElementName(Entry entry) {
        // not use entry.getName() here as "$oid" will be correct to "oid"
        // comment store "$oid", so use comment here
        return entry.getComment();
    }

    private void integrateEntryToRecord(Entry entry, Record.Builder builder, JsonObject json) {
        if (!json.containsKey(getElementName(entry)) || json.isNull(getElementName(entry))) {
            return;
        }
        switch (entry.getType()) {
        case RECORD: {
            final JsonObject jsonObject = json.getJsonObject(getElementName(entry));
            final Record record = convertJsonObjectToRecord(entry.getElementSchema(), jsonObject);
            builder.withRecord(entry, record);
        }
            break;
        case ARRAY:
            final List<?> objects = convertJsonArray(entry.getElementSchema(), json.getJsonArray(getElementName(entry)));
            if (objects != null) {
                builder.withArray(entry, objects);
            }
            break;
        case STRING: {
            String value = json.getString(getElementName(entry));
            builder.withString(entry, value);
        }
            break;
        case INT: {
            int value = json.getInt(getElementName(entry));
            builder.withInt(entry, value);
        }
            break;
        case LONG: {
            long value = json.getJsonNumber(getElementName(entry)).longValue();
            builder.withLong(entry, value);
        }
            break;
        case FLOAT:
        case DOUBLE: {
            double value = json.getJsonNumber(getElementName(entry)).doubleValue();
            builder.withDouble(entry, value);
        }
            break;
        case BOOLEAN: {
            boolean value = json.getBoolean(getElementName(entry));
            builder.withBoolean(entry, value);
        }
            break;
        case BYTES: {
            String value = json.getString(getElementName(entry));
            builder.withBytes(entry, value.getBytes());
        }
            break;
        case DATETIME:
            try {
                String value = json.getString(getElementName(entry));
                builder.withDateTime(entry, ZonedDateTime.parse(value));
            } catch (DateTimeException e) {
                log.error("[convertJsonObjectToRecord] parse ZonedDateTime failed for {} : {}.", getElementName(entry),
                        json.get(getElementName(entry)));
            }
            break;
        }
    }

    /**
     * Extract list of record format element from json array.
     * 
     * @param schema : schema of json array element.
     * @param json : json array.
     * @return list of value.
     */
    private List<? extends Object> convertJsonArray(Schema schema, JsonArray json) {

        final List<? extends Object> result;
        Schema elementSchema = schema.getElementSchema();
        switch (elementSchema.getType()) {
        case RECORD:
            result = json.stream().map((JsonValue v) -> convertJsonObjectToRecord(elementSchema, v.asJsonObject()))
                    .collect(Collectors.toList());
            break;
        case ARRAY:
            result = json.stream().map((JsonValue v) -> convertJsonArray(elementSchema, v.asJsonArray()))
                    .collect(Collectors.toList());

            break;
        case STRING:
            result = json.stream().map(JsonString.class::cast).map(JsonString::getString).collect(Collectors.toList());
            break;
        case LONG:
            result = json.stream().map(JsonNumber.class::cast).map(JsonNumber::longValue).collect(Collectors.toList());
            break;
        case DOUBLE:
            result = json.stream().map(JsonNumber.class::cast).map(JsonNumber::doubleValue).collect(Collectors.toList());
            break;
        case BOOLEAN:
            result = json.stream().map(JsonValue.TRUE::equals).collect(Collectors.toList());
            break;
        default: {
            result = null;
        }
        }

        return result;
    }

    private Type translateType(JsonValue value) {
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
}
