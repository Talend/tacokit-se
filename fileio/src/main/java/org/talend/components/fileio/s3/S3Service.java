package org.talend.components.fileio.s3;

import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

import java.util.List;

import org.talend.components.simplefileio.s3.S3Region;
import org.talend.components.simplefileio.s3.S3RegionUtil;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.Bucket;

@Service
public class S3Service {

    @Suggestions("S3FindBuckets")
    public SuggestionValues findBuckets(@Option("datastore") final S3DataStore dataStore,
            @Option("region") final S3DataSet.S3Region region, @Option("unknownRegion") final String unknownRegion) {
        final AmazonS3 client = createClient(dataStore);
        try {
            final String usedRegion = findRuntimeRegion(region, unknownRegion);
            client.setEndpoint(S3RegionUtil.regionToEndpoint(usedRegion));
            return new SuggestionValues(true,
                    client.listBuckets().stream().map(bucket -> new SuggestionValues.Item(bucket.getName(), bucket.getName()))
                            .sorted(comparing(SuggestionValues.Item::getLabel)).collect(toList()));
        } finally {
            client.shutdown();
        }
    }

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
            } finally {
                client.shutdown();
            }
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, "bucket found");
        } catch (final Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getClass() + " : " + e.getMessage());
        }
    }

    public AmazonS3 createClient(final S3DataStore datastore) {
        final AWSCredentialsProviderChain credentials = datastore.isSpecifyCredentials()
                ? new AWSCredentialsProviderChain(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(datastore.getAccessKey(), datastore.getSecretKey())),
                        new DefaultAWSCredentialsProviderChain(), new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                : new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain(),
                        new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
        return new AmazonS3Client(credentials);
    }

    private boolean doesRegionMatch(final AmazonS3 client, final Bucket bucket, final String usedRegion) {
        try {
            final String bucketLocation = client.getBucketLocation(bucket.getName());
            final String bucketRegion = ofNullable(S3DataSet.S3Region.fromLocation(client.getBucketLocation(bucket.getName())))
                    .map(S3DataSet.S3Region::getValue).orElse(bucketLocation);
            return usedRegion.equals(bucketRegion);
        } catch (final Exception e) {
            return false;
        }
    }

    private String findRuntimeRegion(final S3DataSet.S3Region region, final String alternative) {
        return ofNullable(region).filter(r -> r != S3DataSet.S3Region.OTHER).map(S3DataSet.S3Region::getValue)
                .orElse(alternative);
    }
}
