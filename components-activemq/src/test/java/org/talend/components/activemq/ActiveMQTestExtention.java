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
package org.talend.components.activemq;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.activemq.datastore.ActiveMQDataStore;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Slf4j
public class ActiveMQTestExtention implements ExtensionContext.Store.CloseableResource, BeforeAllCallback, ParameterResolver {

    private final static int ACTIVEMQ_PORT = 61617;

    private TestContext testContext = new TestContext();

    private static final GenericContainer ACTIVEMQ_CONTAINER = new GenericContainer(
            new ImageFromDockerfile().withFileFromClasspath("Dockerfile", "docker/Dockerfile")
                    .withFileFromPath("activemq.xml", Paths.get("src/test/resources/docker/activemq.xml"))
                    .withFileFromPath("broker.ks", Paths.get("src/test/resources/docker/broker.ks")))
                            .withExposedPorts(ACTIVEMQ_PORT)
                            .waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(200)));

    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        log.info("extension before all start");
        if (!started) {
            started = true;
            ACTIVEMQ_CONTAINER.start();
            // The following line registers a callback hook when the root test context is shut down
            extensionContext.getRoot().getStore(GLOBAL).put("any unique name", this);
        }

        String dockerHostAddress = ACTIVEMQ_CONTAINER.getContainerIpAddress();
        Integer activemqHttpPort = ACTIVEMQ_CONTAINER.getMappedPort(ACTIVEMQ_PORT);

        ActiveMQDataStore dataStore = new ActiveMQDataStore();

        dataStore.setHost(dockerHostAddress);
        dataStore.setPort(activemqHttpPort);
        dataStore.setSSL(true);
        testContext.dataStore = dataStore;

        log.info("docker machine: " + dockerHostAddress + ":" + activemqHttpPort);
    }

    @Override
    public void close() {
        log.info("extension close call");
        ACTIVEMQ_CONTAINER.stop();
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
    public static class TestContext {

        private ActiveMQDataStore dataStore;
    }
}