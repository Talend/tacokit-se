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
package org.talend.components.adlsgen2.input;

import java.io.Serializable;
import java.util.Iterator;

import javax.annotation.PostConstruct;
import javax.json.JsonBuilderFactory;

import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.runtime.input.BlobReader;
import org.talend.components.adlsgen2.runtime.input.BlobReader.BlobFileReaderFactory;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Documentation("Azure Data Lake Storage Gen2 Input")
public class AdlsGen2Input implements Serializable {

    @Service
    private final AdlsGen2Service service;

    @Service
    private final RecordBuilderFactory recordBuilderFactory;

    @Service
    private final JsonBuilderFactory jsonFactory;

    private InputConfiguration configuration;

    private Iterator<Record> records;

    private BlobReader reader;

    private AdlsActiveDirectoryService tokenProviderService;

    public AdlsGen2Input(@Option("configuration") final InputConfiguration configuration, final AdlsGen2Service service,
            final RecordBuilderFactory recordBuilderFactory, JsonBuilderFactory jsonFactory,
            final AdlsActiveDirectoryService tokenProviderService) {
        this.configuration = configuration;
        this.service = service;
        this.jsonFactory = jsonFactory;
        this.recordBuilderFactory = recordBuilderFactory;
        this.tokenProviderService = tokenProviderService;
    }

    @PostConstruct
    public void init() {
        log.debug("[init]");
        try {
            reader = BlobFileReaderFactory.getReader(configuration, recordBuilderFactory, jsonFactory, service,
                    tokenProviderService);
        } catch (Exception e) {
            log.error("[init] Error: {}.", e.getMessage());
            throw new AdlsGen2RuntimeException(e.getMessage(), e);
        }
    }

    @Producer
    public Record next() {
        return reader.readRecord();
    }
}
