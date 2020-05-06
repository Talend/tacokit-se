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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.rabbitmq.RabbitMQTestExtention;
import org.talend.components.rabbitmq.configuration.BasicConfiguration;
import org.talend.components.rabbitmq.configuration.ExchangeType;
import org.talend.components.rabbitmq.configuration.ReceiverType;
import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.components.rabbitmq.output.OutputConfiguration;
import org.talend.components.rabbitmq.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.http.client.Client;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.rabbitmq.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.DIRECT_EXCHANGE_NAME;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.FANOUT_EXCHANGE_NAME;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.MAXIMUM_MESSAGES;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.PASSWORD;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.QUEUE_NAME;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.TEST_MESSAGE;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.USER_NAME;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.rabbitmq") // component package
@ExtendWith(RabbitMQTestExtention.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RabbitMQTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory factory;

    @Service
    private RabbitMQService service;

    @Service
    private ActionService actionService;

    private RabbitMQTestExtention.TestContext testContext;

    @BeforeAll
    private void init(RabbitMQTestExtention.TestContext testContext) {
        this.testContext = testContext;
    }

    @Test
    public void sendAndReceiveQueueMessage() {
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        final String outputConfig = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("rabbitmq-output", "RabbitMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("rabbitmq-output").build().run();

        // Receive message from QUEUE_NAME
        final String inputConfig = configurationByExample().forInstance(getInputConfiguration()).configured().toQueryString();

        Job.components().component("rabbitmq-output", "RabbitMQ://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("rabbitmq-output").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void receiveFanoutMessage() throws MalformedURLException, URISyntaxException {
        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
        Client client = new Client(
                "http://" + testContext.getDataStore().getHostname() + ":" + testContext.getHttpPort() + "/api", USER_NAME,
                PASSWORD);

        Thread thread = new Thread(() -> {
            while (true) {
                if (isInputSubscribed(client)) {
                    sendMessageToExchange(outputConfiguration.getBasicConfig().getConnection(), BuiltinExchangeType.FANOUT,
                            FANOUT_EXCHANGE_NAME);
                    break;
                }
            }
        });
        thread.start();

        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
        inputConfiguration.getBasicConfig().setExchangeType(ExchangeType.FANOUT);
        inputConfiguration.getBasicConfig().setExchange(FANOUT_EXCHANGE_NAME);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("rabbitmq-output", "RabbitMQ://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("rabbitmq-output").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");

        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void receiveDirectMessage() throws MalformedURLException, URISyntaxException {
        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
        Client client = new Client(
                "http://" + testContext.getDataStore().getHostname() + ":" + testContext.getHttpPort() + "/api", USER_NAME,
                PASSWORD);

        Thread thread = new Thread(() -> {
            while (true) {
                if (isInputSubscribed(client)) {
                    sendMessageToExchange(outputConfiguration.getBasicConfig().getConnection(), BuiltinExchangeType.DIRECT,
                            DIRECT_EXCHANGE_NAME);
                    break;
                }
            }
        });
        thread.start();

        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
        inputConfiguration.getBasicConfig().setExchangeType(ExchangeType.DIRECT);
        inputConfiguration.getBasicConfig().setExchange(DIRECT_EXCHANGE_NAME);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("rabbitmq-input", "RabbitMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("rabbitmq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");

        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    private void sendMessageToExchange(RabbitMQDataStore store, BuiltinExchangeType exchangeType, String exchangeName) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(store.getHostname());
        factory.setUsername(store.getUserName());
        factory.setPassword(store.getPassword());

        try (Connection connection = service.getConnection(store); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(exchangeName, exchangeType);
            channel.basicPublish(exchangeName, "", null, TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));
        } catch (IOException | TimeoutException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private OutputConfiguration getOutputConfiguration() {
        OutputConfiguration configuration = new OutputConfiguration();
        BasicConfiguration basicConfiguration = getBasicConfiguration();
        configuration.setBasicConfig(basicConfiguration);
        return configuration;
    }

    private BasicConfiguration getBasicConfiguration() {
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        RabbitMQDataStore dataStore = testContext.getDataStore();
        basicConfiguration.setQueue(QUEUE_NAME);
        basicConfiguration.setExchange(FANOUT_EXCHANGE_NAME);
        basicConfiguration.setReceiverType(ReceiverType.QUEUE);
        basicConfiguration.setExchangeType(ExchangeType.FANOUT);
        basicConfiguration.setRoutingKey("");
        basicConfiguration.setConnection(dataStore);
        return basicConfiguration;
    }

    private InputMapperConfiguration getInputConfiguration() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        BasicConfiguration basicConfiguration = getBasicConfiguration();
        configuration.setBasicConfig(basicConfiguration);
        configuration.setMaximumMessages(MAXIMUM_MESSAGES);
        return configuration;
    }

    private boolean isInputSubscribed(Client client) {
        return !client.getConnections().isEmpty()
                && client.getConnection(client.getConnections().get(0).getName()).getChannels() > 0;
    }
}