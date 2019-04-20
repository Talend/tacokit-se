package org.talend.components.solr;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.solr.common.SolrDataStore;
import org.talend.components.solr.common.SolrDataset;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Slf4j
public class SolrTestExtension implements ExtensionContext.Store.CloseableResource, BeforeAllCallback, ParameterResolver {

    private final static int SOLR_PORT = 8983;

    public final static String CORE = "testcore";

    public final static String LOGIN = "solr";

    public final static String PASSWORD = "SolrRocks";

    private TestContext testContext = new TestContext();

    private static final GenericContainer SOLR_CONTAINER = new GenericContainer(new ImageFromDockerfile()
            .withFileFromClasspath("Dockerfile", "solr-ssl-dockerfile/Dockerfile")
            .withFileFromPath("solr-ssl.keystore.jks", Paths.get("src/test/resources/solr-ssl-dockerfile/solr-ssl.keystore.jks"))
            .withFileFromPath("security.json", Paths.get("src/test/resources/solr-ssl-dockerfile/security.json")))
                    .withExposedPorts(SOLR_PORT).waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(200)));

    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        log.info("extension before all start");
        if (!started) {
            started = true;
            SOLR_CONTAINER.start();
            // The following line registers a callback hook when the root test context is shut down
            extensionContext.getRoot().getStore(GLOBAL).put("any unique name", this);
        }

        String dockerHostAddress = SOLR_CONTAINER.getContainerIpAddress();
        String solrHttpPort = String.valueOf(SOLR_CONTAINER.getMappedPort(SOLR_PORT));

        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl("https://" + dockerHostAddress + ":" + solrHttpPort + "/solr/");
        dataStore.setLogin(LOGIN);
        dataStore.setPassword(PASSWORD);

        testContext.solrConnection = new SolrDataset();
        testContext.solrConnection.setCore(CORE);
        testContext.solrConnection.setDataStore(dataStore);

        log.info("docker machine: " + dockerHostAddress + ":" + solrHttpPort);
    }

    @Override
    public void close() {
        log.info("extension close call");
        SOLR_CONTAINER.stop();
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

        private SolrDataset solrConnection;
    }
}
