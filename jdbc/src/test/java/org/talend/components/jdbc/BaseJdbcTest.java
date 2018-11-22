package org.talend.components.jdbc;

import lombok.Data;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Data
public abstract class BaseJdbcTest implements JdbcContainer, BaseTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JdbcService jdbcService;

    @Service
    private I18nMessage i18nMessage;

    @Override
    public JdbcConnection newConnection() {
        final JdbcConnection connection = new JdbcConnection();
        connection.setUserId(getUsername());
        connection.setPassword(getPassword());
        connection.setDbType(getDatabaseType());
        connection.setJdbcUrl(getJdbcUrl());
        return connection;
    }

    public TableNameDataset newTableNameDataset(final String table) {
        TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(newConnection());
        dataset.setTableName(table);
        return dataset;
    }

    public List<Record> readAll(final String table) {
        final TableNameDataset dataset = newTableNameDataset(table);
        final String inConfig = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();
        final List<Record> data = new ArrayList<>(getComponentsHandler().getCollectedData(Record.class));
        getComponentsHandler().resetState();
        return data;
    }

    protected void insertRows(final int rowCount, final boolean withNullValues, final int withMissingIdEvery,
            final String stringPrefix) {
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(getTestTableName()));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?"
                                + rowGeneratorConfig(rowCount, withNullValues, withMissingIdEvery, stringPrefix))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
    }

    /**
     * @return random count between available processor count and available processor *100
     */
    public int getRandomRowCount() {
        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        return new Random().nextInt(((availableProcessors * 100) - availableProcessors)) + availableProcessors;
    }

    public String rowGeneratorConfig(final int rowCount, final boolean withNullValues, final int withMissingIdEvery,
            final String stringPrefix) {
        return "config.rowCount=" + rowCount + "&config.withNullValues=" + withNullValues + "&config.stringPrefix=" + stringPrefix
                + "&config.withMissingIdEvery=" + withMissingIdEvery;
    }

}
