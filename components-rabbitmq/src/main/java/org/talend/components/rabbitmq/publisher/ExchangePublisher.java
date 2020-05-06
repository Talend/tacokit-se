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
package org.talend.components.rabbitmq.publisher;

import com.rabbitmq.client.Channel;
import org.talend.components.rabbitmq.exception.ExchangeDeclareException;
import org.talend.components.rabbitmq.exception.ExchangeDeleteException;
import org.talend.components.rabbitmq.output.ActionOnExchange;
import org.talend.components.rabbitmq.output.OutputConfiguration;
import org.talend.components.rabbitmq.service.I18nMessage;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;

import static org.talend.components.rabbitmq.output.ActionOnExchange.DELETE_AND_CREATE_EXCHANGE;

public class ExchangePublisher implements MessagePublisher {

    @Service
    private I18nMessage i18n;

    private Channel channel;

    private String exchange;

    private String routingKey;

    public ExchangePublisher(Channel channel, OutputConfiguration configuration, final I18nMessage i18nMessage) {
        this.channel = channel;
        this.routingKey = configuration.getBasicConfig().getRoutingKey();
        this.exchange = configuration.getBasicConfig().getExchange();
        this.i18n = i18nMessage;
        onExchange(channel, configuration.getActionOnExchange(), exchange);
        try {
            channel.exchangeDeclare(configuration.getBasicConfig().getExchange(),
                    configuration.getBasicConfig().getExchangeType().getType(), configuration.getBasicConfig().getDurable(),
                    configuration.getBasicConfig().getAutoDelete(), null);
        } catch (IOException e) {
            throw new ExchangeDeclareException(i18n.errorCantDeclareExchange(), e);
        }
    }

    @Override
    public void publish(String message) throws IOException {
        channel.basicPublish(exchange, routingKey, null, message.getBytes());
    }

    private void onExchange(Channel channel, ActionOnExchange action, String exchangeName) {
        try {
            if (action == DELETE_AND_CREATE_EXCHANGE) {
                channel.exchangeDelete(exchangeName);
            }
        } catch (IOException e) {
            throw new ExchangeDeleteException(i18n.errorCantRemoveExchange(), e);
        }
    }
}
