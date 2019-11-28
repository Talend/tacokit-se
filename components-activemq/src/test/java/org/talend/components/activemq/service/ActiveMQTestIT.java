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
package org.talend.components.activemq.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.activemq.ActiveMQTestExtention;
import org.talend.components.activemq.configuration.BasicConfiguration;
import org.talend.components.activemq.configuration.Broker;
import org.talend.components.activemq.configuration.MessageType;
import org.talend.components.activemq.datastore.ActiveMQDataStore;
import org.talend.components.activemq.output.OutputConfiguration;
import org.talend.components.activemq.source.DurableSubscriptionConfiguration;
import org.talend.components.activemq.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.activemq.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.activemq.testutils.ActiveMQTestConstants.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.activemq") // component package
@ExtendWith(ActiveMQTestExtention.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ActiveMQTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory factory;

    private ActiveMQTestExtention.TestContext testContext;

    @BeforeAll
    private void init(ActiveMQTestExtention.TestContext testContext) {
        this.testContext = testContext;
    }

    @Test
    public void sendAndReceiveJMSMessageQueue() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        final String outputConfig = configurationByExample().forInstance(createOutputConfiguration()).configured()
                .toQueryString();
        sendMessageToActiveMQBroker(outputConfig);

        // Receive message from QUEUE
        final String inputConfig = configurationByExample().forInstance(createInputConfiguration()).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageDurableTopic() {
        // Subscribe to Durable TOPIC
        InputMapperConfiguration inputConfiguration = createInputConfiguration();
        inputConfiguration.getSubscriptionConfig().setDurableSubscription(true);
        inputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        // Send message to TOPIC
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = createOutputConfiguration();
        outputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);
        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        sendMessageToActiveMQBroker(outputConfig);

        // Receive message from TOPIC
        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageQueueFailover() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = createOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setFailover(true);
        List<Broker> brokers = createBrokers();
        outputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);

        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        sendMessageToActiveMQBroker(outputConfig);

        // Receive message from QUEUE
        InputMapperConfiguration inputConfiguration = createInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);
        outputConfiguration.getBasicConfig().getConnection().setFailover(true);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageDurableTopicFailover() {
        List<Broker> brokers = createBrokers();

        // Subscribe to Durable TOPIC
        InputMapperConfiguration inputConfiguration = createInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);
        inputConfiguration.getBasicConfig().getConnection().setFailover(true);
        inputConfiguration.getSubscriptionConfig().setDurableSubscription(true);
        inputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        // Send message to TOPIC
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = createOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setFailover(true);

        outputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);
        outputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        sendMessageToActiveMQBroker(outputConfig);

        // Receive message from TOPIC
        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageQueueStaticDiscovery() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = createOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);
        outputConfiguration.getBasicConfig().getConnection().setBrokers(createBrokers());
        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        sendMessageToActiveMQBroker(outputConfig);

        // Receive message from QUEUE
        InputMapperConfiguration inputConfiguration = createInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(createBrokers());
        outputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageDurableTopicStaticDiscovery() {

        // Subscribe to Durable TOPIC
        InputMapperConfiguration inputConfiguration = createInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(createBrokers());
        inputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);
        inputConfiguration.getSubscriptionConfig().setDurableSubscription(true);
        inputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        // Send message to TOPIC
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = createOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);

        outputConfiguration.getBasicConfig().getConnection().setBrokers(createBrokers());
        outputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        sendMessageToActiveMQBroker(outputConfig);

        // Receive message from TOPIC
        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    // @Test
    // public void emptyUrlTest() {
    // InputMapperConfiguration inputConfiguration = createInputConfiguration();
    // inputConfiguration.getBasicConfig().getConnection().setHost("");
    // final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
    //
    // assertThrows(IllegalStateException.class,
    // () -> Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig)
    // .component("collector", "test://collector").connections().from("activemq-input").to("collector").build()
    // .run());
    // }

    @Test
    public void testMessageSelector() {
        // Send Not Persistent message
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));
        final String outputConfig = configurationByExample().forInstance(createOutputConfiguration()).configured()
                .toQueryString();
        sendMessageToActiveMQBroker(outputConfig);

        // Send Persistent message
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE2).build()));
        OutputConfiguration outputConfiguration = createOutputConfiguration();
        outputConfiguration.setDeliveryMode(OutputConfiguration.DeliveryMode.PERSISTENT);
        final String outputConfig2 = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        sendMessageToActiveMQBroker(outputConfig2);

        // Add filter query to receive only PERSISTENT message
        InputMapperConfiguration inputMapperConfiguration = createInputConfiguration();
        inputMapperConfiguration.setMessageSelector("JMSDeliveryMode = 'PERSISTENT'");
        final String inputConfig = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE2, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");

    }

    @Test
    public void testMaximumMessages() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        final String outputConfig2 = configurationByExample().forInstance(createOutputConfiguration()).configured()
                .toQueryString();
        sendMessageToActiveMQBroker(outputConfig2);

        // Forbid message receiving
        InputMapperConfiguration inputMapperConfiguration = createInputConfiguration();
        inputMapperConfiguration.setMaximumMessages(NO_MESSAGES);
        final String inputConfig = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig);

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertTrue(res.isEmpty(), "Component should not process data");

        // Allow message receiving
        InputMapperConfiguration inputMapperConfiguration2 = createInputConfiguration();
        inputMapperConfiguration.setMaximumMessages(TEN_MESSAGES);
        final String inputConfig2 = configurationByExample().forInstance(inputMapperConfiguration2).configured().toQueryString();

        receiveMessageFromActiveMQBroker(inputConfig2);
        final List<JsonObject> res2 = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res2.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");

    }

    private OutputConfiguration createOutputConfiguration() {
        OutputConfiguration configuration = new OutputConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        ActiveMQDataStore dataStore = testContext.getDataStore();
        dataStore.setSSL(true);
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        basicConfiguration.setConnection(dataStore);
        configuration.setBasicConfig(basicConfiguration);
        return configuration;
    }

    private InputMapperConfiguration createInputConfiguration() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        ActiveMQDataStore dataStore = testContext.getDataStore();
        dataStore.setSSL(true);
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        DurableSubscriptionConfiguration subsConfig = new DurableSubscriptionConfiguration();
        subsConfig.setDurableSubscription(DURABLE_SUBSCRIPTION);
        subsConfig.setClientId(CLIENT_ID);
        subsConfig.setSubscriberName(SUBSCRIBER_NAME);
        basicConfiguration.setConnection(dataStore);
        configuration.setSubscriptionConfig(subsConfig);
        configuration.setBasicConfig(basicConfiguration);
        configuration.setMaximumMessages(TEN_MESSAGES);
        configuration.setTimeout(TIMEOUT);
        return configuration;
    }

    private List<Broker> createBrokers() {
        List<Broker> brokers = new ArrayList<>();
        Broker broker1 = new Broker();
        Broker broker2 = new Broker();
        broker1.setHost(WRONG_HOST);
        broker1.setPort(WRONG_PORT);
        broker2.setHost(testContext.getDataStore().getHost());
        broker2.setPort(testContext.getDataStore().getPort());
        brokers.add(broker1);
        brokers.add(broker2);
        return brokers;
    }

    private void sendMessageToActiveMQBroker(String outputConfig) {
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();
    }

    private void receiveMessageFromActiveMQBroker(String inputConfig2) {
        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig2)
                .component("collector", "test://collector").connections().from("activemq-input").to("collector").build().run();
    }

}
