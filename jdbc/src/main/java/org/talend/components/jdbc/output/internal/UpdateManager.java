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
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.talend.components.jdbc.output.OutputConfiguration;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class UpdateManager extends StatementManager {

    private final OutputConfiguration configuration;

    private final String[] updateKeys;

    private final String[] updateValues;

    UpdateManager(final OutputConfiguration dataset, final I18nMessage i18nMessage, final Connection connection) {
        super(i18nMessage, connection);
        this.configuration = dataset;
        this.updateKeys = ofNullable(dataset.getUpdateOperationMapping()).orElse(emptyList()).stream()
                .filter(OutputConfiguration.UpdateOperationMapping::isKey)
                .map(OutputConfiguration.UpdateOperationMapping::getColumn).toArray(String[]::new);
        if (this.updateKeys.length == 0) {
            throw new IllegalStateException(i18n.errorNoKeyForUpdateQuery());
        }

        this.updateValues = ofNullable(dataset.getUpdateOperationMapping()).orElse(emptyList()).stream().filter(m -> !m.isKey())
                .map(OutputConfiguration.UpdateOperationMapping::getColumn).toArray(String[]::new);
        if (this.updateValues.length == 0) {
            throw new IllegalStateException(i18n.errorNoUpdatableColumnWasDefined());
        }

    }

    @Override
    public String createQuery(final Record record) {
        return "UPDATE " + configuration.getDataset().getTableName() + " SET "
                + Stream.of(updateValues).map(c -> c + " = ?").collect(joining(",")) + " WHERE "
                + Stream.of(updateKeys).map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public Map<Schema.Entry, Integer> getSqlQueryParams(final Record record) {
        final Stream<AbstractMap.SimpleEntry<Schema.Entry, Integer>> updateEntries = IntStream
                .rangeClosed(1, this.updateValues.length)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(record.getSchema().getEntries().stream()
                        .filter(e -> e.getName().equals(this.updateValues[i - 1])).findFirst().orElseThrow(
                                () -> new IllegalStateException(this.i18n.errorNoFieldForQueryParam(this.updateValues[i - 1]))),
                        i));

        final Stream<AbstractMap.SimpleEntry<Schema.Entry, Integer>> updateKeysEntries = IntStream
                .rangeClosed(1, updateKeys.length)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(
                        record.getSchema().getEntries().stream().filter(e -> e.getName().equals(updateKeys[i - 1])).findFirst()
                                .orElseThrow(() -> new IllegalStateException(
                                        this.i18n.errorNoFieldForQueryParam(this.updateKeys[i - 1]))),
                        i + this.updateValues.length));

        return Stream.concat(updateEntries, updateKeysEntries)
                .collect(toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

    }

}
