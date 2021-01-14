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

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.StorageException;

import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.exception.ComponentException.ErrorOrigin;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class GSService {

    public static final String ACTION_HEALTH_CHECK = "GOOGLE_STORAGE_HEALTH_CHECK";

    public static final String ACTION_SUGGESTION_BUCKET = "getBuckets";

    public static final String ACTION_SUGGESTION_BLOB = "getBlobs";

    @Service
    private I18nMessage i18n;

    @Service
    private CredentialService credentialService;

    @HealthCheck(ACTION_HEALTH_CHECK)
    public HealthCheckStatus healthCheck(@Option GSDataStore connection) {

        if (connection.getJsonCredentials() == null || "".equals(connection.getJsonCredentials().trim())) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.credentialsRequired());
        }
        try {
            final GoogleCredentials credentials = credentialService.getCredentials(connection.getJsonCredentials());
            final Storage storage = credentialService.newStorage(credentials);
            storage.list();
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
        } catch (final Exception e) {
            final String errorMsg = i18n.errorConnection(e.getMessage());
            log.error(errorMsg, e);
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, errorMsg);
        }
    }

    @Suggestions(ACTION_SUGGESTION_BUCKET)
    public SuggestionValues findBucketsName(@Option("dataStore") GSDataStore dataStore) {
        try {
            final Storage storage = this.newStorage(dataStore);
            final Page<Bucket> buckets = storage.list(BucketListOption.fields(BucketField.NAME));

            return this.retrieveItems(buckets, Bucket::getName);
        } catch (StorageException ex) {
            log.error("google storage exception", ex);
            return new SuggestionValues(false, Collections.emptyList());
        }
    }

    @Suggestions(ACTION_SUGGESTION_BLOB)
    public SuggestionValues findBlobsName(@Option("dataStore") GSDataStore dataStore, @Option("bucket") String bucket) {
        try {
            final Storage storage = this.newStorage(dataStore);
            final Bucket googleBucket = storage.get(bucket);
            if (googleBucket == null) { // bucket not exist.
                return new SuggestionValues(false, Collections.emptyList());
            }
            final Page<Blob> blobs = googleBucket.list(BlobListOption.fields(BlobField.NAME));

            return this.retrieveItems(blobs, Blob::getName);
        } catch (StorageException ex) {
            log.error("google storage exception", ex);
            return new SuggestionValues(false, Collections.emptyList());
        }
    }

    public void checkBucket(StorageFacade storage, String bucketName) {
        if (!storage.isBucketExist(bucketName)) { // bucket does not exist.
            final String errorLabel = this.i18n.bucketUnexist(bucketName);
            log.warn(errorLabel);
            throw new ComponentException(ErrorOrigin.USER, errorLabel);
        }
    }

    public void checkBlob(StorageFacade storage, String bucketName, String blobName) {
        if (!storage.isBlobExist(bucketName, blobName)) { // blob does not exist.
            final String errorLabel = this.i18n.blobUnexist(blobName, bucketName);
            log.warn(errorLabel);
            throw new ComponentException(ErrorOrigin.USER, errorLabel);
        }
    }

    public StorageFacade buildStorage(final String jsonCredentials) {
        return new StorageImpl(this.credentialService, jsonCredentials, this.i18n);
    }

    private Storage newStorage(GSDataStore dataStore) {
        final GoogleCredentials credentials = credentialService.getCredentials(dataStore.getJsonCredentials());
        return credentialService.newStorage(credentials);
    }

    private <T> SuggestionValues retrieveItems(Page<T> pages, Function<T, String> toName) {
        final BlobNameBuilder nameBuilder = new BlobNameBuilder();
        final List<SuggestionValues.Item> names = StreamSupport.stream(pages.iterateAll().spliterator(), false) //
                .map(toName) // T -> name
                .flatMap((String name) -> Stream.of(name, nameBuilder.revert(name)).distinct()).distinct()
                .map((String name) -> new SuggestionValues.Item(name, name)) // name -> suggestion values item.
                .collect(Collectors.toList());
        return new SuggestionValues(!names.isEmpty(), names);
    }
}
