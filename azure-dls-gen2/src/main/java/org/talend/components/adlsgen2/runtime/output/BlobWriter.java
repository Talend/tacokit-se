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
package org.talend.components.adlsgen2.runtime.output;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import javax.json.JsonBuilderFactory;

import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsDatasetRuntimeInfo;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BlobWriter {

    protected final AdlsGen2Service service;

    protected final JsonBuilderFactory jsonFactory;

    protected final RecordBuilderFactory recordBuilderFactory;

    protected final OutputConfiguration configuration;

    protected List<Record> batch;

    protected Schema schema;

    protected BlobInformations currentItem = null;

    protected AdlsDatasetRuntimeInfo runtimeInfo;

    public BlobWriter(OutputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            JsonBuilderFactory jsonFactory, AdlsGen2Service service, AdlsActiveDirectoryService tokenProviderService) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jsonFactory = jsonFactory;
        this.service = service;
        this.runtimeInfo = new AdlsDatasetRuntimeInfo(configuration.getDataSet(), tokenProviderService);
        currentItem = new BlobInformations();
    }

    protected void generateFileWithExtension(String extension) {
        String directoryName = configuration.getDataSet().getBlobPath();
        if (directoryName.startsWith("/") && directoryName.length() > 1) {
            directoryName = directoryName.substring(1);
        }

        if (!directoryName.endsWith("/")) {
            directoryName += "/";
        }
        String blobName = directoryName + configuration.getBlobNameTemplate() + UUID.randomUUID() + extension;
        while (service.blobExists(runtimeInfo, blobName)) {
            blobName = directoryName + configuration.getBlobNameTemplate() + UUID.randomUUID() + extension;
        }
        currentItem.setBlobPath(blobName);
    }

    public void newBatch() {
        batch = new LinkedList<>();
        log.debug("New batch created");
    }

    protected abstract void generateFile();

    public void writeRecord(Record record) {
        if (schema == null) {
            schema = record.getSchema();
        }
        batch.add(record);
    }

    protected void uploadContent(byte[] content) {
        generateFile();
        String oldBlobPath = configuration.getDataSet().getBlobPath();
        configuration.getDataSet().setBlobPath(currentItem.getBlobPath());
        // path create
        service.pathCreate(runtimeInfo);
        long position = 0;
        // update blob
        service.pathUpdate(runtimeInfo, content, position);
        position += content.length; // cumulate length of written records for current offset
        // flush blob
        service.flushBlob(runtimeInfo, position);
        // reset name
        currentItem.setBlobPath("");
        configuration.getDataSet().setBlobPath(oldBlobPath);
    }

    public List<Record> getBatch() {
        return batch;
    }

    protected BlobInformations getCurrentItem() {
        return currentItem;
    }

    protected void setCurrentItem(BlobInformations currentItem) {
        this.currentItem = currentItem;
    }

    protected Schema getSchema() {
        return schema;
    }

    /**
     * Upload prepared batch
     */
    public abstract void flush();

    /**
     * Finish everything
     */
    public void complete() throws Exception {
        if (!getBatch().isEmpty()) {
            log.info("[complete] Executing last batch with {} records", getBatch().size());
            flush();
        }
    }

}
