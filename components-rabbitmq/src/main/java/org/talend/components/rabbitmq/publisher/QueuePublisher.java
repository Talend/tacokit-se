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
import org.talend.components.rabbitmq.exception.QueueDeclareException;
import org.talend.components.rabbitmq.exception.QueueDeleteException;
import org.talend.components.rabbitmq.output.ActionOnQueue;
import org.talend.components.rabbitmq.output.OutputConfiguration;
import org.talend.components.rabbitmq.service.I18nMessage;

import java.io.IOException;

import static org.talend.components.rabbitmq.output.ActionOnQueue.DELETE_AND_CREATE_QUEUE;

public class QueuePublisher implements MessagePublisher {

    private I18nMessage i18n;

    private Channel channel;

    private String queue;

    public QueuePublisher(Channel channel, OutputConfiguration configuration, final I18nMessage i18nMessage) {
        this.channel = channel;
        this.queue = configuration.getBasicConfig().getQueue();
        this.i18n = i18nMessage;
        onQueue(channel, configuration.getActionOnQueue(), queue);
        try {
            channel.queueDeclare(configuration.getBasicConfig().getQueue(), configuration.getBasicConfig().getDurable(), false,
                    configuration.getBasicConfig().getAutoDelete(), null);
        } catch (IOException e) {
            throw new QueueDeclareException(i18n.errorCantDeclareQueue(), e);
        }
    }

    private void onQueue(Channel channel, ActionOnQueue action, String queueName) {
        try {
            if (action == DELETE_AND_CREATE_QUEUE) {
                channel.queueDelete(queueName);
            }
        } catch (IOException e) {
            throw new QueueDeleteException(i18n.errorCantRemoveQueue(), e);
        }
    }

    @Override
    public void publish(String message) throws IOException {
        channel.basicPublish("", queue, null, message.getBytes());
    }
}