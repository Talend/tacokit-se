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
package org.talend.components.google.storage.output;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.Collection;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.service.CredentialService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = IconType.CUSTOM, custom = "cloudstorage")
@Processor(family = "GoogleStorage", name = "Output")
@Documentation("Google storage output")
@RequiredArgsConstructor
public class GoogleStorageOutput implements Serializable {

    private final OutputConfiguration config;

    private final CredentialService credentialService;

    private final RecordIORepository ioRepository;

    private final I18nMessage i18n;

    private RecordWriter recordWriter;

    @AfterGroup
    public void write(final Collection<Record> records) {
        if (this.recordWriter == null) {
            throw new IllegalStateException(this.i18n.outputWasNotInitialize());
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
        final GoogleCredentials googleCredentials = this.credentialService.getCredentials(this.getDataSet().getDataStore());

        // Blob (byte source)
        final Storage storage = this.credentialService.newStorage(googleCredentials);
        final BlobInfo blobInfo = getDataSet().blob();
        Blob blob = storage.get(blobInfo.getBlobId());
        if (blob == null) {
            blob = storage.create(blobInfo);
        }

        // write channel from blob
        final WriteChannel writer = blob.writer();

        // to Record Writer
        this.recordWriter = this.buildWriter(writer);
    }

    private GSDataSet getDataSet() {
        return this.config.getDataset();
    }

    private RecordWriter buildWriter(WriteChannel writerChannel) throws IOException {
        final ContentFormat contentFormat = this.getDataSet().getContentFormat().findFormat();
        final RecordWriterSupplier recordWriterSupplier = this.ioRepository.findWriter(contentFormat.getClass());

        final RecordWriter writer = recordWriterSupplier.getWriter(() -> Channels.newOutputStream(writerChannel), contentFormat);
        writer.init(contentFormat);
        return writer;
    }

}