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
package org.talend.components.adlsgen2.common.format.json;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonIterator implements Iterator<Record>, Serializable {

    private RecordBuilderFactory recordBuilderFactory;

    private JsonConverter converter;

    private JsonReader reader;

    private JsonObject current;

    Iterator<JsonValue> iterator;

    private JsonIterator(JsonConverter converter, JsonReader reader) {
        this.converter = converter;
        this.reader = reader;
        JsonStructure structure = reader.read();
        if (structure == null) {
            iterator = new Iterator<JsonValue>() {

                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public JsonValue next() {
                    return null;
                }
            };
        } else {
            if (structure.getValueType() == ValueType.ARRAY) {
                iterator = structure.asJsonArray().stream().iterator();
            } else {
                List<JsonValue> l = new ArrayList<>();
                l.add(structure.asJsonObject());
                iterator = l.iterator();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Record next() {
        if (hasNext()) {
            return converter.toRecord(iterator.next().asJsonObject());
        } else {
            return null;
        }
    }

    /**
     *
     */
    public static class Builder {

        private JsonConverter converter;

        private JsonConfiguration configuration;

        private RecordBuilderFactory factory;

        private JsonBuilderFactory jsonFactory;

        private Builder(final RecordBuilderFactory factory, final JsonBuilderFactory jsonFactory) {
            this.factory = factory;
            this.jsonFactory = jsonFactory;
        }

        public static JsonIterator.Builder of(final RecordBuilderFactory factory, final JsonBuilderFactory jsonFactory) {
            return new JsonIterator.Builder(factory, jsonFactory);
        }

        public JsonIterator.Builder withConfiguration(final @Configuration("jsonConfiguration") JsonConfiguration configuration) {
            this.configuration = configuration;
            converter = JsonConverter.of(factory, jsonFactory, configuration);

            return this;
        }

        public JsonIterator parse(InputStream in) {
            return new JsonIterator(converter, Json.createReader((new InputStreamReader(in, StandardCharsets.UTF_8))));
        }

        public JsonIterator parse(String content) {
            return new JsonIterator(converter, Json.createReader(new StringReader(content)));
        }
    }
}
