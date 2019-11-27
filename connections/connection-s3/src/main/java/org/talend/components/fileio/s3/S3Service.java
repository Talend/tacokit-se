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
package org.talend.components.fileio.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.util.List;

@Service
public class S3Service {

    private static final Log LOG = LogFactory.getLog(S3Service.class);

    @HealthCheck("S3")
    public HealthCheckStatus healthCheckS3(@Option final S3DataStore dataStore) {
        try {
            final AmazonS3 client = createClient(dataStore);
            try {
                final List<Bucket> buckets = client.listBuckets();
                if (buckets.isEmpty()) {
                    return new HealthCheckStatus(HealthCheckStatus.Status.KO, "No bucket found");
                }
                client.listObjects(buckets.iterator().next().getName(), "any");
            } catch (final AmazonServiceException ase) {
                if (ase.getStatusCode() != Constants.NO_SUCH_BUCKET_STATUS_CODE) {
                    throw ase;
                }
            } catch (final IllegalArgumentException iae) {
                String message = iae.getMessage();
                if (message != null && message.startsWith("Cannot create enum from")) {
                    // caused by low version sdk which support less region, so ignore it here
                } else {
                    throw iae;
                }
            }
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Connection successful");
        } catch (final Exception e) {
            HealthCheckStatus status = new HealthCheckStatus(HealthCheckStatus.Status.KO,
                    e.getClass() + " : " + e.getMessage() + (dataStore.isSpecifyCredentials() ? ""
                            : " : please make sure remote engine is running on EC2 instance that has role with the correct access permission to S3"));
            LOG.info(status.getComment());
            return status;
        }
    }

    private static AWSCredentialsProviderChain createAWSCredentials(S3DataStoreConfiguration datastore) {
        AWSCredentialsProviderChain credentials;
        if (datastore.isSpecifyCredentials()) {
            credentials = new AWSCredentialsProviderChain(
                    new BasicAWSCredentialsProvider(datastore.getAccessKey(), datastore.getSecretKey()),
                    new DefaultAWSCredentialsProviderChain(), new AnonymousAWSCredentialsProvider());
        } else {
            // do not be polluted by hidden accessKey/secretKey
            credentials = new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain(),
                    new AnonymousAWSCredentialsProvider());
        }
        return credentials;
    }

    public static AmazonS3 createClient(S3DataStoreConfiguration datastore) {
        AWSCredentialsProviderChain credentials = S3Service.createAWSCredentials(datastore);
        return AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).withCredentials(credentials).build();
    }

    public static AmazonS3 createClientWithBucketRegion(S3DataStoreConfiguration datastore, final String bucketName) {
        AWSCredentialsProviderChain credentials = S3Service.createAWSCredentials(datastore);
        return AmazonS3ClientBuilder.standard()
                .withRegion(Region.fromValue(createClient(datastore).getBucketLocation(bucketName)).toAWSRegion().getName())
                .withCredentials(credentials).build();
    }

    public static class BasicAWSCredentialsProvider implements AWSCredentialsProvider {

        private final String accessKey;

        private final String secretKey;

        public BasicAWSCredentialsProvider(String accessKey, String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        public AWSCredentials getCredentials() {
            if (this.accessKey != null && !this.accessKey.isEmpty() && this.secretKey != null && !this.secretKey.isEmpty()) {
                return new BasicAWSCredentials(this.accessKey, this.secretKey);
            } else {
                throw new AmazonClientException("Access key or secret key is null");
            }
        }

        public void refresh() {
        }

        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    public static class AnonymousAWSCredentialsProvider implements AWSCredentialsProvider {

        public AnonymousAWSCredentialsProvider() {
        }

        public AWSCredentials getCredentials() {
            return new AnonymousAWSCredentials();
        }

        public void refresh() {
        }

        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

}
