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
package org.talend.components.google.storage.input;

import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.sdk.component.api.record.Record;

public class JsonAllRecordReader implements RecordReader {

    /** converter from json object to record. */
    private final JsonToRecord toRecord;

    public JsonAllRecordReader(JsonToRecord toRecord) {
        this.toRecord = toRecord;
    }

    @Override
    public Iterator<Record> read(InputStream reader) {
        final JsonValue jsonValue = Json.createReader(reader).readValue();
        final Record record = this.convertToRecord(jsonValue);
        return Collections.singletonList(record).iterator();
    }

    @Override
    public void close() {
    }

    /**
     * Convert json value to record
     *
     * @param value : json (wrapped in object if not of JsonObject type).
     * @return Record.
     */
    private Record convertToRecord(JsonValue value) {
        final JsonObject json;
        if (value == null) {
            json = Json.createObjectBuilder().addNull("field").build();
        } else if (value.getValueType() != JsonValue.ValueType.OBJECT) {
            json = Json.createObjectBuilder().add("field", value).build();
        } else {
            json = value.asJsonObject();
        }

        return this.toRecord.toRecord(json);
    }
}
