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

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.InvalidPasswordException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.components.couchbase.source.CouchbaseInput;
import org.talend.components.couchbase.source.CouchbaseInputConfiguration;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.talend.sdk.component.api.record.Schema.Type.*;

@Version(1)
@Slf4j
@Service
public class CouchbaseService {

    private static final transient Logger LOG = LoggerFactory.getLogger(CouchbaseService.class);

    private final Map<CouchbaseDataStore, ClusterHolder> clustersPool = new ConcurrentHashMap<>();

    @Service
    private I18nMessage i18n;

    @Service
    private RecordBuilderFactory builderFactory;

    public String[] resolveAddresses(String nodes) {
        String[] addresses = nodes.replaceAll(" ", "").split(",");
        for (int i = 0; i < addresses.length; i++) {
            LOG.info(i18n.bootstrapNodes(i, addresses[i]));
        }
        return addresses;
    }

    public Cluster openConnection(CouchbaseDataStore dataStore) {
        String bootStrapNodes = dataStore.getBootstrapNodes();
        String username = dataStore.getUsername();
        String password = dataStore.getPassword();
        int connectTimeout = dataStore.getConnectTimeout() * 1000; // convert to sec

        String[] urls = resolveAddresses(bootStrapNodes);
        try {
            ClusterHolder holder = clustersPool.computeIfAbsent(dataStore, ds -> {
                CouchbaseEnvironment environment = new DefaultCouchbaseEnvironment.Builder().connectTimeout(connectTimeout)
                        .build();
                Cluster cluster = CouchbaseCluster.create(environment, urls);
                cluster.authenticate(username, password);
                return new ClusterHolder(environment, cluster);
            });
            holder.use();
            Cluster cluster = holder.getCluster();
            String clusterName = cluster.clusterManager().info().raw().get("name").toString();
            LOG.debug(i18n.connectedToCluster(clusterName));
            return cluster;
        } catch (Exception e) {
            LOG.error(i18n.connectionKO());
            throw new CouchbaseException(e);
        }

    }

    @HealthCheck("healthCheck")
    public HealthCheckStatus healthCheck(@Option("configuration.dataset.connection") final CouchbaseDataStore datastore) {
        try {
            openConnection(datastore);
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Connection OK");
        } catch (Exception exception) {
            String message = "";
            if (exception.getCause() instanceof InvalidPasswordException) {
                message = i18n.invalidPassword();
            } else if (exception.getCause() instanceof RuntimeException
                    && exception.getCause().getCause() instanceof TimeoutException) {
                message = i18n.destinationUnreachable();
            } else {
                message = i18n.connectionKODetailed(exception.getMessage());
            }
            LOG.error(message, exception);
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, message);
        } finally {
            closeConnection(datastore);
        }
    }

    @DiscoverSchema("discover")
    public Schema addColumns(@Option("dataSet") final CouchbaseDataSet dataSet) {
        CouchbaseInputConfiguration configuration = new CouchbaseInputConfiguration();
        configuration.setDataSet(dataSet);
        CouchbaseInput couchbaseInput = new CouchbaseInput(configuration, this, builderFactory, i18n);
        couchbaseInput.init();
        Record record = couchbaseInput.next();
        couchbaseInput.release();

        return record.getSchema();
    }

    public Bucket openBucket(Cluster cluster, String bucketName) {
        Bucket bucket;
        try {
            bucket = cluster.openBucket(bucketName);
        } catch (Exception e) {
            LOG.error(i18n.cannotOpenBucket());
            throw new CouchbaseException(e);
        }
        return bucket;
    }

    public void closeBucket(Bucket bucket) {
        if (bucket != null) {
            if (bucket.close()) {
                LOG.debug(i18n.bucketWasClosed(bucket.name()));
            } else {
                LOG.debug(i18n.cannotCloseBucket(bucket.name()));
            }
        }
    }

    public void closeConnection(CouchbaseDataStore ds) {
        ClusterHolder holder = clustersPool.get(ds);
        if (holder == null) {
            return;
        }
        int stillUsed = holder.release();
        if (stillUsed > 0) {
            return;
        }
        clustersPool.remove(ds);
        Cluster cluster = holder.getCluster();
        CouchbaseEnvironment environment = holder.getEnv();
        if (cluster != null) {
            if (cluster.disconnect()) {
                log.debug(i18n.clusterWasClosed());
            } else {
                log.debug(i18n.cannotCloseCluster());
            }
        }
        if (environment != null) {
            if (environment.shutdown()) {
                log.debug(i18n.couchbaseEnvWasClosed());
            } else {
                log.debug(i18n.cannotCloseCouchbaseEnv());
            }
        }
    }

    public Schema getSchema(JsonObject jsonObject, Set<String> jsonKeys) {
        Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(RECORD);

        if (jsonKeys == null || jsonKeys.isEmpty()) {
            jsonKeys = jsonObject.getNames();
        }

        for (String key : jsonKeys) {
            // receive value from JSON
            Object value = jsonObject.get(key);

            if (value == null) {
                LOG.warn(i18n.cannotGuessWhenDataIsNull());
                continue;
            }

            // With this value we can define type
            Schema.Type type = defineValueType(value);

            // We can add to schema builder entry
            Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withNullable(true).withName(key).withType(type);

            if (type == RECORD) {
                entryBuilder.withElementSchema(getSchema((JsonObject) value, null));
            } else if (type == ARRAY) {
                entryBuilder.withElementSchema(defineSchemaForArray((JsonArray) value));
            }
            Schema.Entry currentEntry = entryBuilder.build();
            schemaBuilder.withEntry(currentEntry);
        }
        return schemaBuilder.build();
    }

    private Schema defineSchemaForArray(JsonArray jsonArray) {
        Object firstValueInArray = jsonArray.get(0);
        Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(RECORD);
        if (firstValueInArray == null) {
            throw new IllegalArgumentException("First value of Array is null. Can't define type of values in array");
        }
        Schema.Type type = defineValueType(firstValueInArray);
        schemaBuilder.withType(type);
        if (type == RECORD) {
            schemaBuilder.withEntry(
                    builderFactory.newEntryBuilder().withElementSchema(getSchema((JsonObject) firstValueInArray, null)).build());
        } else if (type == ARRAY) {
            schemaBuilder.withEntry(builderFactory.newEntryBuilder()
                    .withElementSchema(defineSchemaForArray((JsonArray) firstValueInArray)).build());
        }
        return schemaBuilder.withType(type).build();
    }

    private Schema.Type defineValueType(Object value) {
        if (value instanceof String) {
            return STRING;
        } else if (value instanceof Boolean) {
            return BOOLEAN;
        } else if (value instanceof Date) {
            return DATETIME;
        } else if (value instanceof Double) {
            return DOUBLE;
        } else if (value instanceof Integer) {
            return INT;
        } else if (value instanceof Long) {
            return LONG;
        } else if (value instanceof Byte[]) {
            throw new IllegalArgumentException("BYTES is unsupported");
        } else if (value instanceof JsonArray) {
            return STRING;
        } else if (value instanceof JsonObject) {
            return STRING;
        } else if (value instanceof Float) {
            return FLOAT;
        } else {
            return STRING;
        }
    }

    public static class ClusterHolder {

        @Getter
        private final CouchbaseEnvironment env;

        @Getter
        private final Cluster cluster;

        private final AtomicInteger usages = new AtomicInteger();

        public ClusterHolder(final CouchbaseEnvironment env, final Cluster cluster) {
            this.env = env;
            this.cluster = cluster;
        }

        public void use() {
            usages.incrementAndGet();
        }

        public int release() {
            return usages.decrementAndGet();
        }
    }
}