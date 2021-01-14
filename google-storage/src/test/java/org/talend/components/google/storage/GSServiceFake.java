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
package org.talend.components.google.storage;

import java.io.File;

import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.GSService;
import org.talend.components.google.storage.service.StorageFacade;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GSServiceFake extends GSService {

    private final GSService wrappedService;

    private final File root;

    private final String bucket;

    @Override
    public HealthCheckStatus healthCheck(GSDataStore connection) {
        return this.wrappedService.healthCheck(connection);
    }

    @Override
    public SuggestionValues findBucketsName(GSDataStore dataStore) {
        return this.wrappedService.findBucketsName(dataStore);
    }

    @Override
    public SuggestionValues findBlobsName(GSDataStore dataStore, String bucket) {
        return this.wrappedService.findBlobsName(dataStore, bucket);
    }

    @Override
    public StorageFacade buildStorage(String jsonCredentials) {
        return new StorageFacadeFake(bucket, root);
    }
}
