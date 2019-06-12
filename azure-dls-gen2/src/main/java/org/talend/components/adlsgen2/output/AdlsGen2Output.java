/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import org.talend.components.adlsgen2.output.formatter.ContentFormatter;
import org.talend.components.adlsgen2.output.formatter.ContentFormatterFactory;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.AdlsGen2Service.BlobInformations;
import org.talend.components.adlsgen2.service.I18n;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "azure-datalake")
@Processor(name = "Output")
@Documentation("Azure Data Lake Storage Gen2 Output")
public class AdlsGen2Output implements Serializable {

    @Service
    RecordBuilderFactory recordBuilderFactory;

    @Service
    JsonBuilderFactory jsonBuilderFactory;

    @Service
    private final AdlsGen2Service service;

    @Service
    private final I18n i18n;

    private final ContentFormatter formatter;

    private OutputConfiguration configuration;

    private long position = 0;

    private List<Record> records;

    public AdlsGen2Output(@Option("configuration") final OutputConfiguration configuration, final AdlsGen2Service service,
            final I18n i18n, final RecordBuilderFactory recordBuilderFactory, final JsonBuilderFactory jsonBuilderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18n;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jsonBuilderFactory = jsonBuilderFactory;
        formatter = ContentFormatterFactory.getFormatter(configuration, service, i18n, recordBuilderFactory, jsonBuilderFactory);
        //
        records = new ArrayList<>();
    }

    @PostConstruct
    public void init() {
        //
        log.warn("[init(@PostConstruct)] searching for blob {} .", configuration.getDataSet().getBlobPath());
        BlobInformations blob = service.getBlobInformations(configuration.getDataSet());
        log.info("[init] writing blob {} (exists? {}, overwrite? {}, failOnExisting? {}).", blob.name, blob.isExists(),
                configuration.isBlobOverwrite(), configuration.isFailOnExistingBlob());
        if (configuration.isFailOnExistingBlob() && blob.isExists()) {
            log.warn("[init(@PostConstruct)] will stop process. Config: {}", configuration);
            String msg = i18n.cannotOverwriteBlob(blob.name);
            log.error(msg);
            throw new AdlsGen2RuntimeException(msg);
        }
        // TODO get lease
        position = blob.getContentLength();
        if (configuration.isBlobOverwrite() || !blob.isExists()) {
            service.pathCreate(configuration);
            position = 0;
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        log.warn("[@beforeGroup] record count:{}", records.size());
    }

    @ElementListener
    public void onElement(final Record record) {
        log.warn("[@onElement] [{}] record: {}", records.size(), record);
        records.add(record);
    }

    @AfterGroup
    public void afterGroup() {
        log.warn("[@afterGroup] record count:{}", records.size());
    }

    @PreDestroy
    public void release() {
        log.info("[release@PreDestroy] flushing {} records.", records.size());
        byte[] content = formatter.prepareContent(records);
        Response<JsonObject> response = service.pathUpdate(configuration, content, position);
        records.clear();
        position += content.length; // cumulate length of written records
        response = service.flushBlob(configuration, position);
        // TODO release lease
    }

}
