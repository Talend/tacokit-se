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

import java.sql.Connection;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.talend.components.jdbc.output.OutputConfiguration;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InsertManager extends StatementManager {

    private final OutputConfiguration configuration;

    InsertManager(final OutputConfiguration dataset, final Connection connection, final I18nMessage i18nMessage) {
        super(connection, i18nMessage);
        this.configuration = dataset;
    }

    @Override
    public String createQuery(final Record record) {
        final String[] columns = record.getSchema().getEntries().stream().map(Schema.Entry::getName).toArray(String[]::new);
        final String query = "INSERT INTO " + configuration.getDataset().getTableName()
                + Stream.of(columns).collect(joining(",", "(", ")")) + " VALUES"
                + Stream.of(columns).map(c -> "?").collect(joining(",", "(", ")"));
        log.debug("[query] : " + query);
        return query;
    }

    @Override
    public Map<Schema.Entry, Integer> getSqlQueryParams(final Record record) {
        if (record.getSchema().getEntries().isEmpty()) {
            return Collections.emptyMap();
        }

        return IntStream.range(0, record.getSchema().getEntries().size())
                .mapToObj(index -> new AbstractMap.SimpleEntry<>(record.getSchema().getEntries().get(index), index + 1))
                .collect(toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

}
