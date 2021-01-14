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
package org.talend.components.jdbc.output.statement.operations;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Slf4j
public class Delete extends QueryManagerImpl {

    private final List<String> keys;

    private Map<Integer, Schema.Entry> queryParams;

    private final String query;

    private boolean namedParamsResolved;

    public Delete(final Platform platform, final OutputConfig configuration, final I18nMessage i18n) {
        super(platform, configuration, i18n);
        this.keys = new ArrayList<>(ofNullable(configuration.getKeys()).orElse(emptyList()));
        if (this.keys.isEmpty()) {
            throw new IllegalArgumentException(getI18n().errorNoKeyForDeleteQuery());
        }
        this.query = "DELETE FROM " + getPlatform().identifier(configuration.getDataset().getTableName()) + " WHERE "
                + keys.stream().map(platform::identifier).map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public String buildQuery(final List<Record> records) {
        if (!namedParamsResolved) {
            queryParams = new HashMap<>();
            final AtomicInteger index = new AtomicInteger(0);
            final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                    .collect(toList());
            keys.stream().map(key -> entries.stream().filter(e -> key.equals(e.getName())).findFirst())
                    .filter(Optional::isPresent).map(Optional::get)
                    .forEach(entry -> queryParams.put(index.incrementAndGet(), entry));
            /* can't handle this group without all the named params */
            if (queryParams.size() != keys.size()) {
                final String missingParams = keys.stream()
                        .filter(key -> queryParams.values().stream().noneMatch(e -> e.getName().equals(key)))
                        .collect(joining(","));
                throw new IllegalStateException(new IllegalStateException(getI18n().errorNoFieldForQueryParam(missingParams)));
            }

            namedParamsResolved = true;
        }
        return query;
    }

    @Override
    public boolean validateQueryParam(final Record record) {
        final Set<Schema.Entry> entries = new HashSet<>(record.getSchema().getEntries());
        return keys.stream().allMatch(k -> entries.stream().anyMatch(entry -> entry.getName().equals(k)))
                && entries.stream().filter(entry -> keys.contains(entry.getName())).filter(entry -> !entry.isNullable())
                        .map(entry -> valueOf(record, entry)).allMatch(Optional::isPresent);
    }

    @Override
    public Map<Integer, Schema.Entry> getQueryParams() {
        return queryParams;
    }
}