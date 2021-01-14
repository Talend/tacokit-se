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
package org.talend.components.google.storage.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Collection;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.service.BlobNameBuilder;
import org.talend.components.google.storage.service.GSService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.components.google.storage.service.StorageFacade;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.exception.ComponentException.ErrorOrigin;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = IconType.CUSTOM, custom = "cloudstorage")
@Processor(family = "GoogleStorage", name = "Output")
@Documentation("Google storage output")
@RequiredArgsConstructor
public class GoogleStorageOutput implements Serializable {

    private static final long serialVersionUID = -5829580591413555957L;

    private final OutputConfiguration config;

    private final RecordIORepository ioRepository;

    private final I18nMessage i18n;

    private final GSService services;

    private transient RecordWriter recordWriter;

    @AfterGroup
    public void write(final Collection<Record> records) {
        if (this.recordWriter == null) {
            throw new ComponentException(ErrorOrigin.BACKEND, this.i18n.outputWasNotInitialize());
        }
        try {
            this.recordWriter.add(records);
            this.recordWriter.flush();
        } catch (IOException exIO) {
            String errorLib = this.i18n.writeError(this.getDataSet().getBucket(), this.getDataSet().getBlob(), exIO.getMessage());
            log.error(errorLib);
            throw new UncheckedIOException(errorLib, exIO);
        }
    }

    @PreDestroy
    public void release() {
        try {
            if (this.recordWriter != null) {
                this.recordWriter.end();
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Can't close google storage writer", ex);
        }
    }

    @PostConstruct
    public void init() throws IOException {
        this.services.checkBucket(this.buildStorage(), this.getDataSet().getBucket());
        this.recordWriter = this.buildWriter();
    }

    private GSDataSet getDataSet() {
        return this.config.getDataset();
    }

    private RecordWriter buildWriter() throws IOException {
        final ContentFormat contentFormat = this.getDataSet().getContentFormat().findFormat();
        final RecordWriterSupplier recordWriterSupplier = this.ioRepository.findWriter(contentFormat.getClass());

        final RecordWriter writer = recordWriterSupplier.getWriter(this::buildOutputStream, contentFormat);
        writer.init(contentFormat);
        return writer;
    }

    private OutputStream buildOutputStream() {
        final GSDataSet dataSet = this.getDataSet();

        final StorageFacade storage = this.buildStorage();
        final String blobOutputName = new BlobNameBuilder().generateName(dataSet.getBlob());
        return storage.buildOuput(dataSet.getBucket(), blobOutputName);
    }

    private StorageFacade buildStorage() {
        return this.services.buildStorage(this.getDataSet().getDataStore().getJsonCredentials());
    }

}