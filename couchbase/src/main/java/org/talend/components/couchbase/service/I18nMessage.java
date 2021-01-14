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
package org.talend.components.couchbase.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

    String bootstrapNodes(int num, String address);

    String connectionOK();

    String connectionKO();

    String bucketWasClosed(String bucketName);

    String cannotCloseBucket(String bucketName);

    String clusterWasClosed();

    String cannotCloseCluster();

    String couchbaseEnvWasClosed();

    String cannotCloseCouchbaseEnv();

    String cannotGuessWhenDataIsNull();

    String queryResultError();

    String cannotOpenBucket();

    String connectedToCluster(String clusterName);

    String invalidPassword();

    String destinationUnreachable();

    String connectionKODetailed(String details);
}