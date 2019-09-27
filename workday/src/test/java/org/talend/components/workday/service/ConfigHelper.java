package org.talend.components.workday.service;

import org.talend.components.workday.datastore.WorkdayDataStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

public class ConfigHelper {

    public static Properties workdayProps() {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("workdayConfig.properties")) {
            Properties wkprops = new Properties();
            wkprops.load(in);
            return wkprops;
        }
        catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static WorkdayDataStore buildDataStore() {
        Properties props = ConfigHelper.workdayProps();
        WorkdayDataStore wds = new WorkdayDataStore();
        wds.setClientId(props.getProperty("clientId"));
        wds.setClientSecret(props.getProperty("clientSecret"));
        wds.setTenantAlias(props.getProperty("tenant"));
        wds.setEndpoint(props.getProperty("authendpoint"));
        return wds;
    }
}
