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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public abstract class UpdateTest extends BaseJdbcTest {

    @Test
    @DisplayName("Output [Update] - valid query")
    void update() {
        // insert some initial data
        final int rowCount = getRandomRowCount();
        insertRows(rowCount, false, 0, null);
        // update the inserted data data
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(getTestTableName()));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
        configuration.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, "updated"))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(getTestTableName());
        assertEquals(rowCount, users.size());
        assertEquals(IntStream.rangeClosed(1, rowCount).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                .map(r -> ofNullable(r.getString("T_STRING")).orElseGet(() -> r.getString("t_string"))).collect(toSet()));
    }

    @Test
    @DisplayName("Output [Update] - no keys")
    void updateWithNoKeys() {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration updateConfiguration = new OutputConfiguration();
            updateConfiguration.setDataset(newTableNameDataset("users"));
            updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://RowGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForUpdateQuery()));
    }
}
