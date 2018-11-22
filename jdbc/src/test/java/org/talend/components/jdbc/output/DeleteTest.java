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
package org.talend.components.jdbc.output;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public abstract class DeleteTest extends BaseJdbcTest {

    @Test
    @DisplayName("Output [Delete] - valid query")
    void delete() {
        // insert some initial data
        final int rowCount = getRandomRowCount();
        insertRows(rowCount, false, 0, null);
        // delete the inserted data data
        final OutputConfiguration deleteConfig = new OutputConfiguration();
        deleteConfig.setDataset(newTableNameDataset(getTestTableName()));
        deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
        deleteConfig.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://RowGenerator?config.rowCount=" + rowCount)
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        assertTrue(readAll(getTestTableName()).isEmpty());
    }

    @Test
    @DisplayName("Output [Delete] - No keys")
    void deleteWithNoKeys() {
        final int rowCount = 3;
        insertRows(rowCount, false, 0, null);
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration deleteConfig = new OutputConfiguration();
            deleteConfig.setDataset(newTableNameDataset(getTestTableName()));
            deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
            final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://RowGenerator?config.rowCount=" + rowCount)
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForDeleteQuery()));
        assertEquals(rowCount, readAll(getTestTableName()).size());
    }

    @Test
    @DisplayName("Output [Delete] - Missing defined key in incoming record")
    void deleteWithMissingDefinedKeys() {
        // 1) insert some data.
        final int rowCount = 2;
        insertRows(rowCount, false, 0, null);
        // 2) perform delete test with some record with missing delete key (id)
        final OutputConfiguration deleteConfig = new OutputConfiguration();
        deleteConfig.setDataset(newTableNameDataset(getTestTableName()));
        deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
        deleteConfig.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        final int missingKeyEvery = 2;
        Job.components()
                .component("userGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, missingKeyEvery, null))
                .component("jdbcOutput", "Jdbc://Output?configuration.$maxBatchSize=2&" + updateConfig).connections()
                .from("userGenerator").to("jdbcOutput").build().run();

        // 3) check the remaining records
        assertEquals(IntStream.rangeClosed(1, rowCount).filter(r -> r % missingKeyEvery == 0).count(),
                readAll(getTestTableName()).size());
    }
}
