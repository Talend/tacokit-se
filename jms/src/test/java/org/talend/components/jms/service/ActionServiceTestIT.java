package org.talend.components.jms.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jms.datastore.JmsDataStore;
import org.talend.components.jms.testutils.JMSTestExtention;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.jms.testutils.JmsTestConstants.JMS_PROVIDER;

@WithComponents("org.talend.components.jms") // component package
@ExtendWith(JMSTestExtention.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ActionServiceTestIT {

    @Service
    private ActionService actionService;

    private JMSTestExtention.TestContext testContext;

    @BeforeAll
    private void init(JMSTestExtention.TestContext testContext) {
        this.testContext = testContext;
    }

    @Test
    public void testJMSSuccessfulConnection() throws InterruptedException {

        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setModuleList(JMS_PROVIDER);
        dataStore.setUrl(testContext.getURL());
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

}
