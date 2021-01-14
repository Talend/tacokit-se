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
package org.talend.components.common.stream.input.json;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;

import org.talend.components.common.collections.IteratorMap;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.json.JsonPointerParser;
import org.talend.sdk.component.api.record.Record;

/**
 * Read json object from a stream (Reader) and convert it to Record iterator.
 */
public class JsonRecordReader implements RecordReader {

    /** json pointer config */
    private final JsonPointerParser jsonPointer;

    /** converter from json object to record. */
    private final JsonToRecord toRecord;

    /** current json iterator */
    private JsonParser jsonParser = null;

    public JsonRecordReader(JsonPointerParser jsonPointer, JsonToRecord toRecord) {
        this.jsonPointer = jsonPointer;
        this.toRecord = toRecord;
    }

    @Override
    public Iterator<Record> read(InputStream reader) {
        final Map<String, Object> config = new HashMap<>();
        final JsonParserFactory factory = Json.createParserFactory(config);
        this.jsonParser = factory.createParser(reader);

        final Iterator<JsonValue> values = this.jsonPointer.values(jsonParser);

        return new IteratorMap<>(values, this::convertToRecord);
    }

    @Override
    public void close() {
        if (this.jsonParser != null) {
            this.jsonParser.close();
            this.jsonParser = null;
        }
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
