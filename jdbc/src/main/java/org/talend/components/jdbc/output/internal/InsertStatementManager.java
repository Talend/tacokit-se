/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.output.internal;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InsertStatementManager extends StatementManager {

    private final String tableName;

    private String[] columns;

    private Map<String, Integer> indexedColumns;

    InsertStatementManager(final OutputDataset dataset) {
        super();
        tableName = dataset.getTableName();
    }

    @Override
    public String createQuery(final Record record) {
        columns = record.getSchema().getEntries().stream().map(Schema.Entry::getName).toArray(String[]::new);
        indexedColumns = IntStream.rangeClosed(1, columns.length).mapToObj(i -> new AbstractMap.SimpleEntry<>(columns[i - 1], i))
                .collect(toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        final String query = "INSERT INTO " + tableName + Stream.of(columns).collect(joining(",", "(", ")")) + " VALUES"
                + Stream.of(columns).map(c -> "?").collect(joining(",", "(", ")"));
        log.trace("[query] : " + query);
        return query;
    }

    @Override
    public Map<String, Integer> getIndexedColumns() {
        return indexedColumns;
    }

}
