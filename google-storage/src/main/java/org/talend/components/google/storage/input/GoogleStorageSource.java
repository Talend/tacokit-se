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
package org.talend.components.google.storage.input;

import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.Iterator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.service.CredentialService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "cloudstorageInput")
@Emitter(family = "GoogleStorage", name = "Input")
@Documentation("This component read content file from google cloud storage.")
public class GoogleStorageSource implements Serializable {

    private static final long serialVersionUID = 7373818898514942128L;

    /** google storage input configuration. */
    private final InputConfiguration config;

    /** record factory */
    @Service
    private final RecordBuilderFactory factory;

    @Service
    private final RecordIORepository ioRepository;

    @Service
    private final CredentialService credentialService;

    @Service
    private final I18nMessage i18n;

    private transient GoogleCredentials googleCredentials = null;

    /** current record iterator */
    private transient Iterator<Record> recordIterator = null;

    private transient RecordReader recordReader = null;

    public GoogleStorageSource(@Option("configuration") InputConfiguration config, RecordBuilderFactory factory, // build record
            RecordIORepository ioRepository, // find reader
            CredentialService credentialService, I18nMessage i18n) {
        this.config = config;
        this.factory = factory;
        this.ioRepository = ioRepository;
        this.credentialService = credentialService;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        final GSDataSet dataset = this.getDataSet();
        this.googleCredentials = this.credentialService.getCredentials(dataset.getDataStore());
    }

    @Producer
    public Record next() {
        if (recordIterator == null) {
            this.recordIterator = this.buildRecordIterator();
        }
        if (this.recordIterator != null && this.recordIterator.hasNext()) {
            return recordIterator.next();
        }
        return null;
    }

    @PreDestroy
    public void release() {
        if (this.recordReader != null) {
            this.recordReader.close();
        }
        this.recordIterator = null;
        this.recordReader = null;
    }

    private Iterator<Record> buildRecordIterator() {
        // reader depending on format.
        final GSDataSet dataset = this.getDataSet();
        final ContentFormat format = dataset.getContentFormat().findFormat();

        final RecordReaderSupplier recordReaderSupplier = this.ioRepository.findReader(format.getClass());

        this.recordReader = recordReaderSupplier.getReader(this.factory, format);

        // Blob (byte source)
        final Storage storage = this.credentialService.newStorage(this.googleCredentials);
        final BlobInfo blobInfo = dataset.blob();
        final Blob blob = storage.get(blobInfo.getBlobId());
        if (blob == null) { // blob does not exist.
            final String errorLabel = this.i18n.blobUnexist(dataset.getBlob(), dataset.getBucket());
            throw new IllegalArgumentException(errorLabel);
        }

        // reader & iterator.
        final InputStream in = Channels.newInputStream(blob.reader());
        return this.recordReader.read(in);
    }

    private GSDataSet getDataSet() {
        return this.config.getDataset();
    }

}
