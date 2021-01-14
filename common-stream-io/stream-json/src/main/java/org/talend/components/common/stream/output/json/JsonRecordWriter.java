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
package org.talend.components.common.stream.output.json;

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonGenerator;

import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.sdk.component.api.record.Record;

public class JsonRecordWriter implements RecordWriter {

    private final TargetFinder target;

    private transient JsonGenerator jsonGenerator = null;

    private final RecordConverter<JsonObject, Void> toJson;

    public JsonRecordWriter(TargetFinder target, RecordConverter<JsonObject, Void> toJson) {
        this.target = target;
        this.toJson = toJson;
    }

    @Override
    public void init(ContentFormat config) throws IOException {
        jsonGenerator = Json.createGenerator(this.target.find());
        jsonGenerator.writeStartArray();
    }

    @Override
    public void add(Record record) throws IOException {
        final JsonObject jsonObject = this.toJson.fromRecord(record);
        this.jsonGenerator.write(jsonObject);
    }

    @Override
    public void flush() throws IOException {
        jsonGenerator.flush();
    }

    @Override
    public void close() throws IOException {
        jsonGenerator.writeEnd();
        jsonGenerator.close();
    }
}
