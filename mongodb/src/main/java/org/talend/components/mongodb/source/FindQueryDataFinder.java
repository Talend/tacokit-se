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

package org.talend.components.mongodb.source;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

public class FindQueryDataFinder implements QueryDataFinder<Document> {

    @Override
    public MongoCursor<Document> findData(MongoCollection<Document> collection, MongoDBInputMapperConfiguration configuration) {
        Document myQuery = Document.parse(configuration.getQuery());
        FindIterable<Document> iterable = collection.find(myQuery).noCursorTimeout(configuration.isNoQueryTimeout());
        iterable.limit(configuration.getLimit());
        initSorting(iterable, configuration);
        return iterable.iterator();
    }

    private final void initSorting(final FindIterable<Document> findIterable, MongoDBInputMapperConfiguration configuration) {
        if (configuration.getSort() == null || configuration.getSort().isEmpty()) {
            return;
        }
        BasicDBObject orderBy = new BasicDBObject();
        configuration.getSort().stream()
                .forEach(s -> orderBy.put(s.getColumn(), (s.getOrder() == Sort.SortingOrder.asc) ? 1 : -1));
        findIterable.sort(orderBy);
    }

}
