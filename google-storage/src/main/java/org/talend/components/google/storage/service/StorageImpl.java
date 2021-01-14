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
package org.talend.components.google.storage.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;

import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.exception.ComponentException.ErrorOrigin;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageImpl implements StorageFacade {

    private static final long serialVersionUID = 5310522079015049725L;

    private final GoogleCredentials credentials;

    private final CredentialService credentialService;

    private final I18nMessage i18n;

    private final BlobNameBuilder nameBuilder = new BlobNameBuilder();

    private transient Storage storage = null;

    public StorageImpl(final CredentialService credentialService, final String jsonCredentials, final I18nMessage i18n) {
        this.credentialService = credentialService;
        this.credentials = credentialService.getCredentials(jsonCredentials);
        this.i18n = i18n;
    }

    @Override
    public OutputStream buildOuput(final String bucket, final String blob) {
        final Storage st = this.getStorage();

        final BlobInfo blobInfo = BlobInfo.newBuilder(bucket, blob).build();
        Blob blobObject = st.get(blobInfo.getBlobId());
        if (blobObject == null) {
            blobObject = st.create(blobInfo);
        }

        // write channel from blob
        final WriteChannel writer = blobObject.writer();
        return Channels.newOutputStream(writer);
    }

    @Override
    public Supplier<InputStream> buildInput(final String bucket, final String blob) {
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucket, blob).build();

        final Blob blobObject = this.getStorage().get(blobInfo.getBlobId());
        if (blobObject == null) { // blob does not exist.
            final String errorLabel = this.i18n.blobUnexist(blob, bucket);
            log.warn(errorLabel);
            throw new ComponentException(ErrorOrigin.BACKEND, errorLabel);
        }
        return () -> Channels.newInputStream(blobObject.reader());
    }

    @Override
    public Stream<String> findBlobsName(final String bucket, final String blobStartName) {
        final BlobListOption blobListOption = Storage.BlobListOption.prefix(blobStartName);
        final Page<Blob> blobPage = this.getStorage().list(bucket, blobListOption);

        return StreamSupport.stream(blobPage.iterateAll().spliterator(), false) //
                .map(Blob::getName) //
                .filter((String name) -> Objects.equals(blobStartName, name)
                        || this.nameBuilder.isGenerated(blobStartName, name));
    }

    @Override
    public boolean isBucketExist(String bucketName) {
        try {
            return this.getStorage().get(bucketName) != null;
        } catch (StorageException ex) {
            return false;
        }
    }

    @Override
    public boolean isBlobExist(String bucketName, String blobName) {
        return this.findBlobsName(bucketName, blobName).count() > 0;
    }

    private synchronized Storage getStorage() {
        if (this.storage == null) {
            this.storage = this.credentialService.newStorage(this.credentials);
        }
        return this.storage;
    }

}
