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
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Version
@Slf4j
@Icon(value = Icon.IconType.CUSTOM, custom = "cloudstorage")
@Emitter(family = "GoogleStorage", name = "Input")
@Documentation("This component read content file from google cloud storage.")
@RequiredArgsConstructor
public class GoogleStorageSource implements Serializable {

    private static final long serialVersionUID = 7373818898514942128L;

    /** google storage input configuration. */
    private final InputConfiguration config;

    /** record factory */
    private final RecordBuilderFactory factory;

    private final RecordIORepository ioRepository;

    private final CredentialService credentialService;

    private final I18nMessage i18n;

    private transient GoogleCredentials googleCredentials = null;

    /** current record iterator */
    private transient Iterator<Record> recordIterator = null;

    private transient RecordReader recordReader = null;

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
        final ContentFormat format = this.getDataSet().getContentFormat().findFormat();
        final RecordReaderSupplier recordReaderSupplier = this.ioRepository.findReader(format.getClass());
        this.recordReader = recordReaderSupplier.getReader(this.factory, format);

        // find source blob.
        final Blob blob = this.findBlob();

        // reader & iterator.
        final InputStream in = Channels.newInputStream(blob.reader());
        return this.recordReader.read(in);
    }

    /**
     * Get dataset targeted blob.
     * 
     * @return google storage blob.
     */
    private Blob findBlob() {
        final GSDataSet dataset = this.getDataSet();
        // Blob (byte source)
        final Storage storage = this.credentialService.newStorage(this.googleCredentials);
        final BlobInfo blobInfo = dataset.blob();

        this.checkBucket(storage, dataset.getBucket());

        final Blob blob = storage.get(blobInfo.getBlobId());
        if (blob == null) { // blob does not exist.
            final String errorLabel = this.i18n.blobUnexist(dataset.getBlob(), dataset.getBucket());
            log.warn(errorLabel);
            throw new IllegalArgumentException(errorLabel);
        }
        return blob;
    }

    /**
     * Check bucket existence.
     * Exception if not exists.
     * (put in specific protected method for test reason; can't create bucket with google fake storage class).
     * 
     * @param storage : storage.
     * @param bucketName : name of bucket.
     */
    protected void checkBucket(Storage storage, String bucketName) {
        final Bucket bucket = storage.get(bucketName);
        if (bucket == null) { // bucket does not exist.
            final String errorLabel = this.i18n.bucketUnexist(bucketName);
            log.warn(errorLabel);
            throw new IllegalArgumentException(errorLabel);
        }
    }

    private GSDataSet getDataSet() {
        return this.config.getDataset();
    }
}
