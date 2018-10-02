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

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.sql.Connection;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class DeleteStatementManager extends StatementManager {

    private final OutputDataset dataset;

    private final String[] deleteKeys;

    DeleteStatementManager(final OutputDataset dataset, final Connection connection, final I18nMessage i18nMessage) {
        super(connection, i18nMessage);
        this.dataset = dataset;
        this.deleteKeys = ofNullable(dataset.getDeleteKeys()).orElse(emptyList()).stream().filter(Objects::nonNull)
                .filter(key -> !key.isEmpty()).map(String::trim).toArray(String[]::new);
        if (this.deleteKeys.length == 0) {
            throw new IllegalStateException(this.i18n.errorNoKeyForDeleteQuery());
        }

    }

    @Override
    public String createQuery(final Record record) {
        return "DELETE FROM " + dataset.getTableName() + " WHERE "
                + Stream.of(deleteKeys).map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public Map<Schema.Entry, Integer> getSqlQueryParams(final Record record) {
        return IntStream.rangeClosed(1, deleteKeys.length)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(
                        record.getSchema().getEntries().stream().filter(e -> e.getName().equals(deleteKeys[i - 1])).findFirst()
                                .orElseThrow(() -> new IllegalStateException(i18n.errorNoFieldForQueryParam(deleteKeys[i - 1]))),
                        i))
                .collect(toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }
}
