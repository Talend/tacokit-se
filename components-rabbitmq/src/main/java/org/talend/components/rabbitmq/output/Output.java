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
package org.talend.components.rabbitmq.output;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import javax.json.JsonObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.talend.components.rabbitmq.publisher.ExchangePublisher;
import org.talend.components.rabbitmq.publisher.MessagePublisher;
import org.talend.components.rabbitmq.publisher.QueuePublisher;
import org.talend.components.rabbitmq.service.I18nMessage;
import org.talend.components.rabbitmq.service.RabbitMQService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;

import static org.talend.components.rabbitmq.MessageConst.MESSAGE_CONTENT;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "RabbitMQOutput")
@Processor(name = "Output")
@Documentation("Main class for ActiveMQOutput records processing")
public class Output implements Serializable {

    private final OutputConfiguration configuration;

    private final I18nMessage i18n;

    private final RabbitMQService service;

    private Connection connection;

    private Channel channel;

    private MessagePublisher publisher;

    public Output(@Option("configuration") final OutputConfiguration configuration, final RabbitMQService service,
            final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() throws IOException, TimeoutException, NoSuchAlgorithmException {
        connection = service.getConnection(configuration.getBasicConfig().getConnection());
        channel = service.createChannel(connection);

        switch (configuration.getBasicConfig().getReceiverType()) {
        case QUEUE:
            publisher = new QueuePublisher(channel, configuration, i18n);
            break;
        case EXCHANGE:
            publisher = new ExchangePublisher(channel, configuration, i18n);
            break;
        }
    }

    @ElementListener
    public void onNext(@Input final JsonObject record) throws IOException {
        publisher.publish(getMessage(record));
    }

    private String getMessage(JsonObject record) {
        return record.getString(MESSAGE_CONTENT);
    }

    @PreDestroy
    public void release() {
        service.closeConnection(connection);
    }
}