package org.talend.components.zendesk;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.zendesk.common.SelectionType;
import org.talend.components.zendesk.common.ZendeskDataSet;
import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.components.zendesk.service.ZendeskService;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@DisplayName("Suite of test for the Zendesk components")
@WithComponents("org.talend.components.zendesk")
@ExtendWith(ZendeskTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZendeskServiceTestIT {

    @Service
    private ZendeskService zendeskService;

    private ZendeskTestExtension.TestContext testContext;

    @BeforeAll
    private void init(ZendeskTestExtension.TestContext testContext) {
        log.info("init service test");
        this.testContext = testContext;
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Health check")
    void healthCheckTest(ZendeskDataStore dataStoreCustom) {
        log.info("Integration test 'Health Check' start " + dataStoreCustom);
        HealthCheckStatus healthCheckStatus = zendeskService.validateBasicConnection(dataStoreCustom);
        assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }

    @Test
    @DisplayName("Health check bad")
    void healthCheckTestBad() {
        log.info("Integration test 'Health Check bad' start ");
        ZendeskDataStore dataStoreCustom = new ZendeskDataStore();
        HealthCheckStatus healthCheckStatus = zendeskService.validateBasicConnection(dataStoreCustom);
        assertEquals(HealthCheckStatus.Status.KO, healthCheckStatus.getStatus());
    }

    @ParameterizedTest
    @MethodSource("methodSourceSelectionType")
    @DisplayName("Schema discovery")
    void schemaDiscoveryTest(SelectionType selectionType) {
        log.info("Integration test 'Schema discovery'. Selection type: " + selectionType);
        ZendeskDataSet dataSet = new ZendeskDataSet();
        dataSet.setSelectionType(selectionType);

        dataSet.setDataStore(testContext.getDataStoreLoginPassword());

        Schema schema = zendeskService.guessTableSchema(dataSet);
        Assertions.assertTrue(schema.getEntries().stream().map(item -> item.getName()).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "subject", "description")));
    }

    private Stream<Arguments> methodSourceSelectionType() {
        return Stream.of(Arguments.of(SelectionType.TICKETS));
    }

    private Stream<Arguments> methodSourceDataStores() {
        return testContext.getDataStores();
    }
}
