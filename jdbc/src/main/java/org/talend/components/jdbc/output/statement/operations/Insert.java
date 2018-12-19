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
package org.talend.components.jdbc.output.statement.operations;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.*;

@Slf4j
public class Insert extends QueryManager {

    private Map<Integer, Schema.Entry> namedParams;

    private final Map<String, String> queries = new HashMap<>();

    public Insert(final Platform platform, final OutputConfig configuration, final I18nMessage i18n,
            final JdbcService.JdbcDatasource dataSource) {
        super(platform, configuration, i18n, dataSource);
    }

    @Override
    public String buildQuery(final List<Record> records) {
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        return queries.computeIfAbsent(entries.stream().map(Schema.Entry::getName).collect(joining("::")), key -> {
            final AtomicInteger index = new AtomicInteger(0);
            namedParams = new HashMap<>();
            entries.forEach(name -> namedParams.put(index.incrementAndGet(), name));
            final List<Map.Entry<Integer, Schema.Entry>> params = namedParams.entrySet().stream()
                    .sorted(comparing(Map.Entry::getKey)).collect(toList());
            final StringBuilder query = new StringBuilder("INSERT INTO ")
                    .append(getPlatform().identifier(getConfiguration().getDataset().getTableName()));
            query.append(params.stream().map(e -> e.getValue().getName()).map(name -> getPlatform().identifier(name))
                    .collect(joining(",", "(", ")")));
            query.append(" VALUES");
            query.append(params.stream().map(e -> "?").collect((joining(",", "(", ")"))));
            return query.toString();
        });
    }

    @Override
    public boolean validateQueryParam(final Record record) {
        return record.getSchema().getEntries().stream().map(Schema.Entry::getName).collect(toSet()).containsAll(namedParams
                .values().stream().filter(namedParam -> !namedParam.isNullable()).map(Schema.Entry::getName).collect(toSet()));
    }

    @Override
    public Map<Integer, Schema.Entry> getQueryParams() {
        return namedParams;
    }
}