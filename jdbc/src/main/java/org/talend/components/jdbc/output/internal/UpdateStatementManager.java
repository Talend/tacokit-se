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

public class UpdateStatementManager extends StatementManager {

    private final String tableName;

    private final String[] updateValues;

    private final String[] updateKeys;

    private final Map<String, Integer> indexedColumns;

    UpdateStatementManager(final OutputDataset dataset) {
        super();
        tableName = dataset.getTableName();
        updateKeys = dataset.getUpdateOperationMapping().stream().filter(OutputDataset.UpdateOperationMapping::isKey)
                .map(OutputDataset.UpdateOperationMapping::getColumn).toArray(String[]::new);

        if (updateKeys.length == 0) {
            throw new IllegalStateException("Please select the update keys.");
        }

        updateValues = dataset.getUpdateOperationMapping().stream().filter(m -> !m.isKey())
                .map(OutputDataset.UpdateOperationMapping::getColumn).toArray(String[]::new);

        if (updateValues.length == 0) {
            throw new IllegalStateException("Please select the columns  to be updated.");
        }

        indexedColumns = Stream
                .concat(IntStream.rangeClosed(1, updateValues.length)
                        .mapToObj(i -> new AbstractMap.SimpleEntry<>(updateValues[i - 1], i)),
                        IntStream.rangeClosed(1, updateKeys.length)
                                .mapToObj(i -> new AbstractMap.SimpleEntry<>(updateKeys[i - 1], i + updateValues.length)))
                .collect(toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    @Override
    public String createQuery(final Record record) {
        return "UPDATE " + tableName + " SET " + Stream.of(updateValues).map(c -> c + " = ?").collect(joining(",")) + " WHERE "
                + Stream.of(updateKeys).map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public Map<String, Integer> getIndexedColumns() {
        return indexedColumns;
    }

}
