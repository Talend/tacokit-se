package org.talend.components.jdbc.output;

import static java.util.stream.Collectors.toList;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;

import javax.json.JsonObject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithDerby(onStartSQLScript = "derby/output_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents("org.talend.components.jdbc") // component package
public class OutputTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Test
    @DisplayName("Execute a valid insert")
    void insert(final DerbyExtension.DerbyInfo derbyInfo) {

        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        final OutputDataset dataset = new OutputDataset();
        dataset.setConnection(connection);
        dataset.setActionOnData(OutputDataset.ActionOnData.Insert);
        dataset.setTableName("user");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components()
                .component("RowGenerator", "test://emitter")
                .component("jdbcOutput", "Jdbc://Output?" + config)
                .connections()
                .from("RowGenerator").to("jdbcOutput").build().run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertNotNull(records);
        assertEquals(4, records.size());
        assertEquals(asList("user1", "user2", "user3", "user4"),
                records.stream().map(r -> r.getString("NAME")).collect(toList()));
    }

}
