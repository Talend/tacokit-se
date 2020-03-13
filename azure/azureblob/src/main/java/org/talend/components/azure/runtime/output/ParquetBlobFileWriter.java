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
package org.talend.components.azure.runtime.output;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.runtime.converters.ParquetConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class ParquetBlobFileWriter extends BlobFileWriter {

    private BlobOutputConfiguration config;

    private ParquetConverter converter;

    public ParquetBlobFileWriter(BlobOutputConfiguration config, AzureBlobComponentServices connectionServices) throws Exception {
        super(config, connectionServices);
        this.config = config;
        this.converter = ParquetConverter.of(null);
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
    public void generateFile(String directoryName) throws URISyntaxException, StorageException {
        String fileName = directoryName + config.getBlobNameTemplate() + System.currentTimeMillis() + ".parquet";

        CloudBlob blob = getContainer().getBlockBlobReference(fileName);
        while (blob.exists(null, null, AzureComponentServices.getTalendOperationContext())) {
            fileName = directoryName + config.getBlobNameTemplate() + System.currentTimeMillis() + ".parquet";
            blob = getContainer().getBlockBlobReference(fileName);
        }

        setCurrentItem(blob);
    }

    @Override
    public void flush() {
        if (getBatch().isEmpty()) {
            return;
        }

        File tempFilePath = null;
        try {
            tempFilePath = File.createTempFile("tempFile", ".parquet");
            Path tempFile = new org.apache.hadoop.fs.Path(tempFilePath.getPath());
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord> builder(tempFile)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withSchema(converter.inferAvroSchema(getSchema())).build();
            for (Record r : getBatch()) {
                writer.write(converter.fromRecord(r));
            }

            writer.close();
            OutputStream blobOutputStream = ((CloudBlockBlob) getCurrentItem()).openOutputStream();
            Files.copy(tempFilePath.toPath(), blobOutputStream);
            blobOutputStream.flush();
            blobOutputStream.close();
        } catch (IOException | StorageException e) {
            throw new BlobRuntimeException(e);
        } finally {
            getBatch().clear();
            if (tempFilePath != null) {
                tempFilePath.delete();
            }
        }
    }
}
