/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.rabbitmq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.HOSTNAME;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.PASSWORD;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.PORT;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.USER_NAME;

@WithComponents("org.talend.components.rabbitmq")
class ActionServiceTest {

    @Service
    private ActionService actionService;

    @Test
    public void testNoConnection() {
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname(HOSTNAME);
        dataStore.setPort(Integer.valueOf(PORT));
        dataStore.setUserName(USER_NAME);
        dataStore.setPassword(PASSWORD);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    public void testConnectionEmptyHostname() {
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname("");
        dataStore.setPort(Integer.valueOf(PORT));
        assertThrows(IllegalArgumentException.class, () -> {
            actionService.validateBasicDatastore(dataStore);
        });
    }

    @Test
    public void testConnectionEmptyPort() {
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname(HOSTNAME);
        dataStore.setPort(null);
        assertThrows(IllegalArgumentException.class, () -> {
            actionService.validateBasicDatastore(dataStore);
        });
    }

}
