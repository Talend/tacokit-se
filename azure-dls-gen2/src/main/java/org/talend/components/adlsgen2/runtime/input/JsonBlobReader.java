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
package org.talend.components.adlsgen2.runtime.input;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.talend.components.adlsgen2.common.format.json.JsonConverter;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonBlobReader extends BlobReader {

    private JsonBuilderFactory jsonFactoryBuilder;

    JsonBlobReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            JsonBuilderFactory jsonFactoryBuilder, AdlsGen2Service service, AdlsActiveDirectoryService tokenProviderService) {
        super(configuration, recordBuilderFactory, service, tokenProviderService);
        this.jsonFactoryBuilder = jsonFactoryBuilder;
    }

    @Override
    protected RecordIterator initRecordIterator(Iterable<BlobInformations> blobItems) {
        return new JsonFileRecordIterator(blobItems, recordBuilderFactory, jsonFactoryBuilder);
    }

    @Override
    public Record readRecord() {
        return super.readRecord();
    }

    private class JsonFileRecordIterator extends RecordIterator<JsonValue> {

        private InputStream currentItemInputStream;

        private Iterator<JsonValue> jsonRecordIterator;

        private JsonConverter converter;

        private JsonBuilderFactory jsonFactory;

        private JsonReader reader;

        protected JsonFileRecordIterator(Iterable<BlobInformations> blobList, RecordBuilderFactory recordBuilderFactory,
                JsonBuilderFactory jsonBuilderFactory) {
            super(blobList, recordBuilderFactory);
            jsonFactory = jsonFactoryBuilder;
            peekFirstBlob();
        }

        @Override
        protected void readBlob() {
            initMetadataIfNeeded();
            closePreviousInputStream();
            try {
                currentItemInputStream = service.getBlobInputstream(datasetRuntimeInfo, getCurrentBlob());
                reader = Json.createReader((new InputStreamReader(currentItemInputStream, StandardCharsets.UTF_8)));
                JsonStructure structure = reader.read();
                if (structure == null) {
                    jsonRecordIterator = new Iterator<JsonValue>() {

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
                        jsonRecordIterator = structure.asJsonArray().stream().iterator();
                    } else {
                        List<JsonValue> l = new ArrayList<>();
                        l.add(structure.asJsonObject());
                        jsonRecordIterator = l.iterator();
                    }
                }
            } catch (Exception e) {
                log.error("[readBlob] {}", e.getMessage());
                throw new AdlsGen2RuntimeException(e.getMessage());
            }
        }

        @Override
        protected boolean hasNextBlobRecord() {
            return jsonRecordIterator.hasNext();
        }

        @Override
        protected Record convertToRecord(JsonValue next) {
            return converter.toRecord(next.asJsonObject());
        }

        @Override
        protected JsonValue peekNextBlobRecord() {
            return jsonRecordIterator.next();
        }

        @Override
        protected void complete() {
            closePreviousInputStream();
        }

        private void initMetadataIfNeeded() {
            if (converter == null) {
                converter = JsonConverter.of(getRecordBuilderFactory(), jsonFactoryBuilder,
                        configuration.getDataSet().getJsonConfiguration());
            }
        }

        private void closePreviousInputStream() {
            if (currentItemInputStream != null) {
                try {
                    currentItemInputStream.close();
                } catch (IOException e) {
                    log.error("Can't close stream: {}.", e.getMessage());
                }
            }
        }
    }
}
