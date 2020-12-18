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
package org.talend.components.common.stream.output.json;

import java.time.ZonedDateTime;
import java.util.Collection;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

import lombok.extern.slf4j.Slf4j;

/**
 * Transform record to json object.
 * here, currently no need of json schema.
 * if needed, could use https://github.com/leadpony/justify
 * (java lib for json schema, can be used with johnzon lib.
 */
@Slf4j
public class RecordToJson implements RecordConverter<JsonObject, Void> {

    @Override
    public JsonObject fromRecord(Record record) {

        if (record == null) {
            return null;
        }
        return convertRecordToJsonObject(record);
    }

    @Override
    public Void fromRecordSchema(Schema record) {
        return null;
    }

    private JsonObject convertRecordToJsonObject(Record record) {
        final JsonObjectBuilder json = Json.createObjectBuilder();

        for (Entry entry : record.getSchema().getEntries()) {
            final String fieldName = entry.getName();
            Object val = record.get(Object.class, fieldName);
            log.debug("[convertRecordToJsonObject] entry: {}; type: {}; value: {}.", fieldName, entry.getType(), val);
            if (null == val) {
                json.addNull(fieldName);
            } else {
                this.addField(json, record, entry);
            }
        }
        return json.build();
    }

    private JsonArray toJsonArray(Collection<Object> objects) {
        JsonArrayBuilder array = Json.createArrayBuilder();
        for (Object obj : objects) {
            if (obj instanceof Collection) {
                JsonArray subArray = toJsonArray((Collection) obj);
                array.add(subArray);
            } else if (obj instanceof String) {
                array.add((String) obj);
            } else if (obj instanceof Record) {
                JsonObject subObject = convertRecordToJsonObject((Record) obj);
                array.add(subObject);
            } else if (obj instanceof Integer) {
                array.add((Integer) obj);
            } else if (obj instanceof Long) {
                array.add((Long) obj);
            } else if (obj instanceof Double) {
                array.add((Double) obj);
            } else if (obj instanceof Boolean) {
                array.add((Boolean) obj);
            }
        }
        return array.build();
    }

    private void addField(JsonObjectBuilder json, Record record, Entry entry) {
        final String fieldName = entry.getName();
        switch (entry.getType()) {
        case RECORD:
            final Record subRecord = record.getRecord(fieldName);
            json.add(fieldName, convertRecordToJsonObject(subRecord));
            break;
        case ARRAY:
            final Collection<Object> array = record.getArray(Object.class, fieldName);
            final JsonArray jarray = toJsonArray(array);
            json.add(fieldName, jarray);
            break;
        case STRING:
            json.add(fieldName, record.getString(fieldName));
            break;
        case BYTES:
            json.add(fieldName, new String(record.getBytes(fieldName)));
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
            final ZonedDateTime dateTime = record.getDateTime(fieldName);
            if (dateTime != null) {
                json.add(fieldName, dateTime.toString());
            } else {
                json.addNull(fieldName);
            }
            break;
        }
    }

}
