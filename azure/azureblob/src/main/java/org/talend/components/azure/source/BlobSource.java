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
package org.talend.components.azure.source;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.runtime.input.BlobFileReader;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Documentation("Azure Blob Storage reader")
public class BlobSource implements Serializable {

    private final BlobInputProperties configuration;

    private final AzureBlobComponentServices service;

    private final RecordBuilderFactory builderFactory;

    private final MessageService messageService;

    private BlobFileReader reader;

    public BlobSource(@Option("configuration") final BlobInputProperties configuration, final AzureBlobComponentServices service,
            final RecordBuilderFactory builderFactory, final MessageService i18n) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.messageService = i18n;
    }

    @PostConstruct
    public void init() throws Exception {
        try {
            reader = BlobFileReader.BlobFileReaderFactory.getReader(configuration.getDataset(), builderFactory, service,
                    messageService);
        } catch (Exception e) {
            throw new BlobRuntimeException(messageService.cantStartReadBlobItems(e.getMessage()), e);
        }
    }

    @Producer
    public Record next() {
        return reader.readRecord();
    }
}