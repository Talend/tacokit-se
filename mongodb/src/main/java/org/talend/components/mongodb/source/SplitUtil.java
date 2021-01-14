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
package org.talend.components.mongodb.source;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.talend.components.mongodb.dataset.BaseDataSet;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.service.MongoDBService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class SplitUtil {

    public static List<String> getQueries4Split(final BaseSourceConfiguration configuration, final MongoDBService service,
            final int splitCount) {
        List<String> result = new ArrayList<>();

        BaseDataSet dataset = configuration.getDataset();
        MongoDBDataStore datastore = dataset.getDatastore();

        MongoClient client = null;
        try {
            client = service.createClient(datastore);
            MongoDatabase database = client.getDatabase(datastore.getDatabase());
            MongoCollection<Document> collection = database.getCollection(dataset.getCollection());

            result = getQueries4Split(collection, splitCount);
        } catch (Exception e) {
            // ignore any exception for split, for example: main node can't reason mongodb
            log.info(e.getMessage(), e);
        } finally {
            service.closeClient(client);
        }

        return result;
    }

    public static long getEstimatedSizeBytes(final BaseSourceConfiguration configuration, final MongoDBService service) {
        BaseDataSet dataset = configuration.getDataset();
        MongoDBDataStore datastore = dataset.getDatastore();

        MongoClient client = null;
        try {
            client = service.createClient(datastore);
            MongoDatabase database = client.getDatabase(datastore.getDatabase());
            BasicDBObject stat = new BasicDBObject();
            stat.append("collStats", dataset.getCollection());
            Document stats = database.runCommand(stat);

            return stats.get("size", Number.class).longValue();
        } catch (Exception e) {
            // ignore any exception for split, for example: main node can't reason mongodb
            log.info(e.getMessage(), e);
        } finally {
            service.closeClient(client);
        }

        return 1l;
    }

    // https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/
    private static List<String> getQueries4Split(final MongoCollection mongoCollection, final int splitCount) {
        List<String> result = new ArrayList<>();

        BsonDocument bucketAutoConfig = new BsonDocument();
        bucketAutoConfig.put("groupBy", new BsonString("$_id"));
        bucketAutoConfig.put("buckets", new BsonInt32(splitCount));
        BsonDocument bucketAuto = new BsonDocument("$bucketAuto", bucketAutoConfig);
        List<BsonDocument> aggregates = new ArrayList<>();
        aggregates.add(bucketAuto);
        AggregateIterable<Document> buckets = mongoCollection.aggregate(aggregates).allowDiskUse(true);

        // [1,3) [3,5) and so on
        Iterator<Document> iterator = buckets.iterator();
        while (iterator.hasNext()) {
            Document bucket = iterator.next();
            Document id_document = (Document) bucket.get("_id");
            Object min = id_document.get("min");
            Object max = id_document.get("max");

            Bson filters = Filters.and(Filters.gte("_id", min),
                    iterator.hasNext() ? Filters.lt("_id", max) : Filters.lte("_id", max));
            String filtersShellJson = filtersToJson(filters);
            result.add(filtersShellJson);
        }

        return result;
    }

    private static String filtersToJson(Bson filters) {
        BsonDocument document = filters.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        return document.toJson(JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
    }

    public static boolean isSplit(String query, Long limit) {
        // limit is passed by platform, so mean getSample here, so no split as we will set limit for query
        if (limit != null && limit > 0) {
            return false;
        }

        // have set filter, then not split now, maybe should too?
        if (query == null || query.trim().isEmpty() || "{}".equals(query)) {
            return true;
        }

        return false;
    }

    public static boolean isSplit(Long limit) {
        // limit is passed by platform, so mean getSample here, so no split as we will set limit for query
        if (limit != null && limit > 0) {
            return false;
        }

        return true;
    }

}
