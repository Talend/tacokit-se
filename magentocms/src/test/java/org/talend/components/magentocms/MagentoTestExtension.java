/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.magentocms;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.magentocms.common.AuthenticationOauth1Configuration;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.RestVersion;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Slf4j
public class MagentoTestExtension implements ExtensionContext.Store.CloseableResource, BeforeAllCallback, ParameterResolver {

    /**
     * get this variables from Magento's docker image.
     * http://MAGENTO_URL/admin -> system -> integrations -> TalendTest -> Edit -> Integration Details
     */
    private static final String AUTHENTICATION_OAUTH1_CONSUMER_KEY = "7fqa5rplt4k9dubdbfea17mf3owyteqh";

    private static final String AUTHENTICATION_OAUTH1_CONSUMER_SECRET = "cpln0ehi2yh7tg5ho9bvlbyprfi0ukqk";

    private static final String AUTHENTICATION_OAUTH1_ACCESS_TOKEN = "j24y53g83te2fgye8fe8xondubqej4cl";

    private static final String AUTHENTICATION_OAUTH1_ACCESS_TOKEN_SECRET = "jxnbv58bc94dfsld1c9k7e6tvcqntrx2";

    private TestContext testContext = new TestContext();

    private static final GenericContainer MAGENTO_CONTAINER = new GenericContainer(new ImageFromDockerfile()
            .withDockerfileFromBuilder(
                    builder -> builder.from("registry.datapwn.com/sbovsunovskyi/components-integration-test-magentocms:1.0.0")
                            .copy("docker.cer", "/etc/ssl/certs/ssl-cert-snakeoil.pem")
                            .copy("docker.key", "/etc/ssl/private/ssl-cert-snakeoil.key").build())
            .withFileFromPath("docker.cer", Paths.get("docker/cert/test_docker.cer"))
            .withFileFromPath("docker.key", Paths.get("docker/cert/test_docker.key"))).withExposedPorts(80, 443)
                    .withEnv("MAGENTO_BASE_URL", "http://192.168.99.100:30080")
                    .withEnv("MAGENTO_BASE_URL_SECURE", "https://192.168.99.100:30443").withEnv("MAGENTO_USE_SECURE", "0")
                    .withEnv("MAGENTO_USE_SECURE_ADMIN", "0")
                    .waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(200)));

    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        log.info("extension before all start");
        if (!started) {
            started = true;
            MAGENTO_CONTAINER.start();
            // The following line registers a callback hook when the root test context is shut down
            extensionContext.getRoot().getStore(GLOBAL).put("any unique name", this);
        }

        testContext.dockerHostAddress = MAGENTO_CONTAINER.getContainerIpAddress();
        testContext.magentoHttpPort = String.valueOf(MAGENTO_CONTAINER.getMappedPort(80));
        testContext.magentoHttpPortSecure = String.valueOf(MAGENTO_CONTAINER.getMappedPort(443));
        testContext.magentoAdminName = System.getProperty("magentoAdminName");
        testContext.magentoAdminPassword = System.getProperty("magentoAdminPassword");

        log.info("docker machine: " + testContext.dockerHostAddress + ":" + testContext.magentoHttpPort);
        log.info("docker machine secure: " + testContext.dockerHostAddress + ":" + testContext.magentoHttpPortSecure);

        AuthenticationLoginPasswordConfiguration authenticationSettings = new AuthenticationLoginPasswordConfiguration(
                testContext.magentoAdminName, testContext.magentoAdminPassword);
        AuthenticationOauth1Configuration authenticationOauth1Settings = new AuthenticationOauth1Configuration(
                AUTHENTICATION_OAUTH1_CONSUMER_KEY, AUTHENTICATION_OAUTH1_CONSUMER_SECRET, AUTHENTICATION_OAUTH1_ACCESS_TOKEN,
                AUTHENTICATION_OAUTH1_ACCESS_TOKEN_SECRET);
        testContext.dataStore = new MagentoDataStore(getBaseUrl(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null, null,
                authenticationSettings);
        testContext.dataStoreSecure = new MagentoDataStore(getBaseUrlSecure(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD,
                null, null, authenticationSettings);
        testContext.dataStoreOauth1 = new MagentoDataStore(getBaseUrl(), RestVersion.V1, AuthenticationType.OAUTH_1,
                authenticationOauth1Settings, null, null);
    }

    @Override
    public void close() {
        log.info("extension close call");
        MAGENTO_CONTAINER.stop();
    }

    private String getBaseUrl() {
        return "http://" + testContext.dockerHostAddress
                + (testContext.magentoHttpPort.isEmpty() || testContext.magentoHttpPort.equals("80") ? ""
                        : ":" + testContext.magentoHttpPort);
    }

    private String getBaseUrlSecure() {
        return "https://" + testContext.dockerHostAddress
                + (testContext.magentoHttpPortSecure.isEmpty() || testContext.magentoHttpPortSecure.equals("443") ? ""
                        : ":" + testContext.magentoHttpPortSecure);
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

        private MagentoDataStore dataStore;

        private MagentoDataStore dataStoreSecure;

        private MagentoDataStore dataStoreOauth1;

        private String dockerHostAddress;

        private String magentoHttpPort;

        private String magentoHttpPortSecure;

        private String magentoAdminName;

        private String magentoAdminPassword;
    }
}
