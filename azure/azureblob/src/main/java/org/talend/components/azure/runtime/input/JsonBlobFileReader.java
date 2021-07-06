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
package org.talend.components.azure.runtime.input;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;

import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonBlobFileReader extends BlobFileReader {

    public JsonBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices, MessageService messageService)
            throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices, messageService);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new JsonRecordIterator(blobItems, getRecordBuilderFactory());
    }

    private class JsonRecordIterator extends ItemRecordIterator<JsonValue> {

        private Iterator<JsonValue> jsonRecordIterator;

        private final JsonToRecord converter;

        private InputStream input;

        private JsonReader reader;

        public JsonRecordIterator(Iterable<ListBlobItem> blobItems, RecordBuilderFactory recordBuilderFactory) {
            super(blobItems, recordBuilderFactory);
            this.converter = new JsonToRecord(recordBuilderFactory);
            takeFirstItem();
        }

        @Override
        protected JsonValue takeNextRecord() {
            return jsonRecordIterator.next();
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return jsonRecordIterator.hasNext();
        }

        @Override
        protected Record convertToRecord(JsonValue next) {
            return converter.toRecord(next.asJsonObject());
        }

        @Override
        protected void readItem() {
            // initMetadataIfNeeded();
            closePreviousInputStream();
            try {
                input = getCurrentItem().openInputStream();
                reader = Json.createReader((new InputStreamReader(input, StandardCharsets.UTF_8)));
                JsonStructure structure = reader.read();
                if (structure == null) {
                    jsonRecordIterator = Collections.emptyIterator();
                } else {
                    if (structure.getValueType() == JsonValue.ValueType.ARRAY) {
                        jsonRecordIterator = structure.asJsonArray().stream().iterator();
                    } else {
                        List<JsonValue> l = new ArrayList<>();
                        l.add(structure.asJsonObject());
                        jsonRecordIterator = l.iterator();
                    }
                }
            } catch (Exception e) {
                log.error("[readBlob] {}", e.getMessage());
                throw new BlobRuntimeException(e.getMessage());
            }
        }

        @Override
        protected void complete() {
            closePreviousInputStream();
        }

        private void closePreviousInputStream() {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    log.error("Can't close stream: {}.", e.getMessage());
                }
            }
        }
    }
}
