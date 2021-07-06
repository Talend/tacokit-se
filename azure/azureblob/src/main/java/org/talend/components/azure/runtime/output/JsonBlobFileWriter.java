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
package org.talend.components.azure.runtime.output;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;

import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.common.stream.output.json.RecordToJson;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;

public class JsonBlobFileWriter extends BlobFileWriter {

    private final BlobOutputConfiguration config;

    private final RecordToJson converter;

    private JsonBuilderFactory jsonBuilderFactory;

    public JsonBlobFileWriter(BlobOutputConfiguration config, AzureBlobComponentServices connectionServices) throws Exception {
        super(config, connectionServices);
        this.config = config;

        this.converter = new RecordToJson();
        this.jsonBuilderFactory = connectionServices.getJsonBuilderFactory();
    }

    @Override
    public void newBatch() {
        super.newBatch();

        try {
            generateFile();
        } catch (Exception e) {
            throw new BlobRuntimeException(e);
        }
    }

    @Override
    protected void generateFile(String directoryName) throws URISyntaxException, StorageException {
        String fileName = directoryName + config.getBlobNameTemplate() + UUID.randomUUID() + ".json";
        CloudBlob blob = getContainer().getBlockBlobReference(fileName);
        while (blob.exists(null, null, AzureComponentServices.getTalendOperationContext())) {
            fileName = directoryName + config.getBlobNameTemplate() + UUID.randomUUID() + ".json";
            blob = getContainer().getBlockBlobReference(fileName);
        }

        setCurrentItem(blob);
    }

    @Override
    public void flush() throws IOException, StorageException {
        byte[] batchBytes = convertBatchToBytes();
        getCurrentItem().uploadFromByteArray(batchBytes, 0, batchBytes.length);
        getBatch().clear();
    }

    private byte[] convertBatchToBytes() {
        JsonArrayBuilder b = jsonBuilderFactory.createArrayBuilder();
        for (Record record : getBatch()) {

            b.add(converter.fromRecord(record));
        }

        return b.build().asJsonArray().toString().getBytes();
    }
}
