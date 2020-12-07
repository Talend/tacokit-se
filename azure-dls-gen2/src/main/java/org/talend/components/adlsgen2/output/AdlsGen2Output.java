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
package org.talend.components.adlsgen2.output;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;

import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.runtime.output.BlobWriter;
import org.talend.components.adlsgen2.runtime.output.BlobWriterFactory;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
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
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "AdlsGen2Output")
@Processor(name = "AdlsGen2Output")
@Documentation("Azure Data Lake Storage Gen2 Output")
public class AdlsGen2Output implements Serializable {

    @Service
    RecordBuilderFactory recordBuilderFactory;

    @Service
    private final AdlsActiveDirectoryService tokenProviderService;

    @Service
    JsonBuilderFactory jsonBuilderFactory;

    @Service
    private final AdlsGen2Service service;

    private OutputConfiguration configuration;

    private BlobWriter blobWriter;

    public AdlsGen2Output(@Option("configuration") final OutputConfiguration configuration, final AdlsGen2Service service,
            final RecordBuilderFactory recordBuilderFactory, final JsonBuilderFactory jsonBuilderFactory,
            AdlsActiveDirectoryService tokenProviderService) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.tokenProviderService = tokenProviderService;
    }

    @PostConstruct
    public void init() {
        log.debug("[init]");
        try {
            blobWriter = BlobWriterFactory.getWriter(configuration, recordBuilderFactory, jsonBuilderFactory, service,
                    tokenProviderService);
        } catch (Exception e) {
            log.error("[init] {}", e.getMessage());
            throw new AdlsGen2RuntimeException(e.getMessage(), e);
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        blobWriter.newBatch();
    }

    @ElementListener
    public void onNext(@Input final Record record) {
        // skip empty record
        if (record != null && record.getSchema().getEntries().isEmpty()) {
            log.info("[onNext] Skipping empty record.");
            return;
        }

        blobWriter.writeRecord(record);
    }

    @AfterGroup
    public void afterGroup() {
        log.debug("[afterGroup] flushing {} records.", blobWriter.getBatch().size());
        try {
            blobWriter.flush();
        } catch (Exception e) {
            log.error("[afterGroup] {}", e.getMessage());
            throw new AdlsGen2RuntimeException(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void release() {
        log.debug("[release]");
        try {
            blobWriter.complete();
        } catch (Exception e) {
            log.error("[release] {}", e.getMessage());
            throw new AdlsGen2RuntimeException(e.getMessage(), e);
        }
    }

}
