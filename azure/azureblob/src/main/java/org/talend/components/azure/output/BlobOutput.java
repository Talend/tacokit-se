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
package org.talend.components.azure.output;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.runtime.output.BlobFileWriter;
import org.talend.components.azure.runtime.output.BlobFileWriterFactory;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-azure")
@Processor(name = "Output")
@Documentation("Azure Blob Storage Writer")
public class BlobOutput implements Serializable {

    private final BlobOutputConfiguration configuration;

    private final AzureBlobComponentServices service;

    private final MessageService messageService;

    private BlobFileWriter fileWriter;

    public BlobOutput(@Option("configuration") final BlobOutputConfiguration configuration,
            final AzureBlobComponentServices service, final MessageService i18n) {
        this.configuration = configuration;
        this.service = service;
        this.messageService = i18n;
    }

    @PostConstruct
    public void init() {
        try {
            this.fileWriter = BlobFileWriterFactory.getWriter(configuration, service);
        } catch (Exception e) {
            throw new BlobRuntimeException(messageService.errorCreateBlobItem(), e);
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        fileWriter.newBatch();
    }

    @ElementListener
    public void onNext(@Input final Record defaultInput) {
        fileWriter.writeRecord(defaultInput);
    }

    @AfterGroup
    public void afterGroup() {
        try {
            fileWriter.flush();
        } catch (Exception e) {
            throw new BlobRuntimeException(messageService.errorSubmitRows(), e);
        }
    }

    @PreDestroy
    public void release() {
        try {
            if (fileWriter != null) {
                fileWriter.complete();
            }
        } catch (Exception e) {
            throw new BlobRuntimeException(messageService.errorSubmitRows(), e);
        }
    }
}