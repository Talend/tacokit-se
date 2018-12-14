package org.talend.components.salesforce;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.salesforce.service.SalesforceService.URL;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.output.OutputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.Data;

@Data
@WithComponents("org.talend.components.salesforce")
public class SalesforceTestBase implements Serializable {

    public static String USER_ID;

    public static String PASSWORD;

    public static String SECURITY_KEY;

    static {
        final MavenDecrypter decrypter = new MavenDecrypter();
        final Server serverWithPassword = decrypter.find("salesforce-password");
        final Server serverWithSecuritykey = decrypter.find("salesforce-securitykey");
        USER_ID = serverWithPassword.getUsername();
        PASSWORD = serverWithPassword.getPassword();
        SECURITY_KEY = serverWithSecuritykey.getPassword();
    }

    @Injected
    private BaseComponentsHandler componentsHandler;

    protected void cleanTestRecords(String module, String condition) {

        final ModuleDataSet inputDataSet = new ModuleDataSet();
        inputDataSet.setModuleName(module);
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id"));
        inputDataSet.setColumnSelectionConfig(selectionConfig);
        inputDataSet.setDataStore(getDataStore());
        inputDataSet.setCondition(condition);
        final String inputConfig = configurationByExample().forInstance(inputDataSet).configured().toQueryString();

        // output
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet outDataSet = new ModuleDataSet();
        outDataSet.setModuleName(module);
        outDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.DELETE);
        configuration.setModuleDataSet(outDataSet);

        final String outputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + inputConfig)
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + outputConfig).connections()
                .from("salesforce-input").to("salesforce-output").build().run();
        getComponentsHandler().resetState();
    }

    protected void checkModuleData(String module, String condition, int expected) {
        final ModuleDataSet dataSet = new ModuleDataSet();
        dataSet.setModuleName(module);
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(singletonList("Id"));
        dataSet.setColumnSelectionConfig(selectionConfig);
        dataSet.setDataStore(getDataStore());
        dataSet.setCondition(condition);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertEquals(expected, res.size());
        getComponentsHandler().resetState();
    }

    public BasicDataStore getDataStore() {
        BasicDataStore dataStore = new BasicDataStore();
        dataStore.setEndpoint(URL);
        dataStore.setUserId(USER_ID);
        dataStore.setPassword(PASSWORD);
        dataStore.setSecurityKey(SECURITY_KEY);
        return dataStore;
    }

}