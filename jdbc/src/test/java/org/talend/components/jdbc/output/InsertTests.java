package org.talend.components.jdbc.output;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public abstract class InsertTests extends BaseJdbcTest {

    @Test
    @DisplayName("Output [Insert] - valid use case")
    void insert() {
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(getTestTableName()));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = getRandomRowCount();
        Job.components().component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();
        final List<Record> records = readAll(getTestTableName());
        assertNotNull(records);
        assertEquals(rowCount, records.size());
    }

    @Test
    @DisplayName("Output [Insert] - with null values")
    void insertWithNullValues() {
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(getTestTableName()));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = getRandomRowCount();
        Job.components().component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, true, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();
        final List<Record> records = readAll(getTestTableName());
        assertNotNull(records);
        assertEquals(rowCount, records.size());
    }

    @Test
    @DisplayName("Output [Insert] - duplicate records")
    void insertDuplicateRecords() {
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(getTestTableName()));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = 1;
        Job.components().component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();
        final List<Record> records = readAll(getTestTableName());
        assertNotNull(records);
        assertEquals(rowCount, records.size());
        assertThrows(Exception.class,
                () -> Job.components()
                        .component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                        .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput")
                        .build().run());
        assertEquals(rowCount, records.size());// assert no new record was inserted
    }

}
