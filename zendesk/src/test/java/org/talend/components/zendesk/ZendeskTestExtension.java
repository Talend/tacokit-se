package org.talend.components.zendesk;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.params.provider.Arguments;
import org.talend.components.zendesk.common.AuthenticationApiTokenConfiguration;
import org.talend.components.zendesk.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.zendesk.common.AuthenticationType;
import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.stream.Stream;

@Slf4j
public class ZendeskTestExtension implements BeforeAllCallback, ParameterResolver {

    private String SERVER_URL;

    private final String MAVEN_SERVER_NAME = "zendesk";

    private final String MAVEN_SERVER_NAME_TOKEN = "zendesk-token";

    private TestContext testContext = new TestContext();

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws IOException {
        log.info("extension before all start");

        readPropertiesFile();
        testContext.dataStoreLoginPassword = getDataStoreFromConfig(MAVEN_SERVER_NAME);
        testContext.dataStoreToken = getDataStoreFromConfig(MAVEN_SERVER_NAME_TOKEN);
    }

    private ZendeskDataStore getDataStoreFromConfig(String serverName) {
        Server zendeskServer = new MavenDecrypter().find(serverName);
        if (serverName.equals(MAVEN_SERVER_NAME)) {
            AuthenticationLoginPasswordConfiguration authenticationSettings = new AuthenticationLoginPasswordConfiguration(
                    zendeskServer.getUsername(), zendeskServer.getPassword());
            return new ZendeskDataStore(SERVER_URL, AuthenticationType.LOGIN_PASSWORD, authenticationSettings, null);
        } else if (serverName.equals(MAVEN_SERVER_NAME_TOKEN)) {
            AuthenticationApiTokenConfiguration authenticationSettings = new AuthenticationApiTokenConfiguration(
                    zendeskServer.getUsername(), zendeskServer.getPassword());
            return new ZendeskDataStore(SERVER_URL, AuthenticationType.API_TOKEN, null, authenticationSettings);
        }
        throw new UnsupportedOperationException("Unknown server config section name");
    }

    private void readPropertiesFile() throws IOException {
        try (InputStream is = ClassLoader.getSystemResourceAsStream("connection.properties")) {
            Properties props = new Properties();
            props.load(is);
            SERVER_URL = props.getProperty("serverURL", "");
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

        private ZendeskDataStore dataStoreLoginPassword;

        private ZendeskDataStore dataStoreToken;

        public Stream<Arguments> getDataStores() {
            return Stream.of(Arguments.of(dataStoreLoginPassword), Arguments.of(dataStoreToken));
        }
    }
}
