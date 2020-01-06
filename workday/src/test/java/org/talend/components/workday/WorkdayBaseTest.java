package org.talend.components.workday;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@WithMavenServers
public class WorkdayBaseTest {
    public static final String defaultAuthenticationURL = "https://auth.api.workday.com";

    public static final String defaultServiceURL = "https://api.workday.com";

    @DecryptedServer(value = "workday.account")
    protected Server serverWorkday;

    @DecryptedServer(value = "workday.tenant")
    protected Server serverWorkdayTenant;

    protected Properties workdayProps() {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("workdayConfig.properties")) {
            Properties wkprops = new Properties();
            wkprops.load(in);
            return wkprops;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    protected WorkdayDataStore buildDataStore() {
        Properties props = this.workdayProps();
        WorkdayDataStore wds = new WorkdayDataStore();
        wds.setClientId(serverWorkday.getUsername());
        wds.setClientSecret(serverWorkday.getPassword());

        // tenant
        wds.setTenantAlias(serverWorkdayTenant.getUsername());

        wds.setAuthEndpoint(props.getProperty("authendpoint"));
        wds.setEndpoint(props.getProperty("endpoint"));
        return wds;
    }
}
