package org.talend.components.onedrive;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataSet;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class OneDriveTestExtension implements BeforeAllCallback, ParameterResolver {

    private String TENANT_ID;

    private String APPLICATION_ID;

    private final String MAVEN_SERVER_NAME = "onedrive";

    private TestContext testContext = new TestContext();

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws IOException {
        log.info("extension before all start");
        log.info("NEXUS_USER" + System.getenv("NEXUS_USER"));
        log.info("onedrive-integration-user:" + System.getenv("ONEDRIVE_INTEGRATION_USER"));

        // for (Map.Entry<String, String> s: System.getenv().entrySet()) {
        // log.info(s.getKey() + ":" + s.getValue());
        // }

        readPropertiesFile();

        Server oneDriveServer = new MavenDecrypter().find(MAVEN_SERVER_NAME);
        AuthenticationLoginPasswordConfiguration authenticationSettings = new AuthenticationLoginPasswordConfiguration();
        authenticationSettings.setAuthenticationLogin(oneDriveServer.getUsername());
        authenticationSettings.setAuthenticationPassword(oneDriveServer.getPassword());
        OneDriveDataStore dataStoreLoginPassword = new OneDriveDataStore();
        dataStoreLoginPassword.setTenantId(TENANT_ID);
        dataStoreLoginPassword.setApplicationId(APPLICATION_ID);
        dataStoreLoginPassword.setAuthenticationType(AuthenticationType.LOGIN_PASSWORD);
        dataStoreLoginPassword.setAuthenticationLoginPasswordConfiguration(authenticationSettings);
        testContext.dataSetLoginPassword = new OneDriveDataSet();
        testContext.dataSetLoginPassword.setDataStore(dataStoreLoginPassword);

    }

    private void readPropertiesFile() throws IOException {
        try (InputStream is = ClassLoader.getSystemResourceAsStream("connection.properties")) {
            Properties props = new Properties();
            props.load(is);
            TENANT_ID = props.getProperty("tenant_id", "");
            APPLICATION_ID = props.getProperty("application_id", "");
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return TestContext.class.equals(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return testContext;
    }

    @Data
    static class TestContext {

        private OneDriveDataSet dataSetLoginPassword;

    }
}
