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