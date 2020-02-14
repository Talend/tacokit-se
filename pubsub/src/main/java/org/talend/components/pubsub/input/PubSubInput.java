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
package org.talend.components.pubsub.input;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.input.converter.MessageConverter;
import org.talend.components.pubsub.input.converter.MessageConverterFactory;
import org.talend.components.pubsub.service.AckMessageService;
import org.talend.components.pubsub.service.DefaultAckReplyConsumer;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.components.pubsub.service.PubSubService;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
@RequiredArgsConstructor
public class PubSubInput implements MessageReceiver, Serializable {

    protected final PubSubInputConfiguration configuration;

    protected final PubSubService service;

    protected final AckMessageService ackMessageService;

    protected final I18nMessage i18n;

    protected final RecordBuilderFactory builderFactory;

    protected final MessageConverterFactory messageConverterFactory;

    private transient final Queue<PubsubMessage> inbox = new ConcurrentLinkedDeque<>();

    /** Subscriber (asynchronous mode only) */
    private transient Subscriber subscriber;

    /** Subscriber (synchronous mode only) */
    private transient SubscriberStub subscriberStub;

    private transient MessageConverter messageConverter;

    @PostConstruct
    public void init() {
        messageConverter = messageConverterFactory.getConverter(configuration.getDataSet(), builderFactory, i18n);
        if (configuration.getPullMode() == PubSubInputConfiguration.PullMode.ASYNCHRONOUS) {
            subscriber = service.createSubscriber(configuration.getDataSet().getDataStore(),
                    configuration.getDataSet().getTopic(), configuration.getDataSet().getSubscription(), this);
            subscriber.startAsync();
        } else {
            subscriberStub = service.createSubscriber(configuration.getDataSet().getDataStore(),
                    configuration.getDataSet().getTopic(), configuration.getDataSet().getSubscription());

        }
    }

    @PreDestroy
    public void release() {
        if (!inbox.isEmpty()) {
            log.debug(i18n.inputReleaseWithMessageInbox(inbox.size()));
            inbox.stream().map(PubsubMessage::getMessageId).forEach(ackMessageService::removeMessage);
        }
        if (subscriber != null) {
            subscriber.stopAsync();
        }
        if (subscriberStub != null) {
            subscriberStub.close();
        }
    }

    @Producer
    public Object next() {
        if (inbox.isEmpty() && configuration.getPullMode() == PubSubInputConfiguration.PullMode.SYNCHRONOUS) {
            pull();
        }

        PubsubMessage message = inbox.poll();

        Object record = null;
        if (message != null && (!configuration.isConsumeMsg() || ackMessageService.messageExists(message.getMessageId()))) {
            try {
                record = messageConverter == null ? null : messageConverter.convertMessage(message);
            } catch (Exception e) {
                log.warn(i18n.warnReadMessage(message.getMessageId(), e.getMessage()), e);
            }

            if (configuration.isConsumeMsg()) {
                ackMessageService.ackMessage(message.getMessageId());
            }
        }

        return record;
    }

    public void pull() {
        PullRequest pullRequest = PullRequest.newBuilder().setMaxMessages(configuration.getMaxMsg()).setReturnImmediately(true)
                .setSubscription(ProjectSubscriptionName.format(configuration.getDataSet().getDataStore().getProjectName(),
                        configuration.getDataSet().getSubscription()))
                .build();

        PullResponse pullResponse = subscriberStub.pullCallable().call(pullRequest);
        pullResponse.getReceivedMessagesList().stream().forEach(rm -> {
            inbox.offer(rm.getMessage());
            if (configuration.isConsumeMsg()) {
                ackMessageService.addMessage(rm.getMessage(),
                        new DefaultAckReplyConsumer.Builder().setAckMessageService(ackMessageService)
                                .setSubscriberStub(subscriberStub).setDataStore(configuration.getDataSet().getDataStore())
                                .setTopic(configuration.getDataSet().getTopic())
                                .setSubscriptionId(configuration.getDataSet().getSubscription()).setAckId(rm.getAckId()).build());
            }
        });
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        inbox.offer(message);
        if (configuration.isConsumeMsg()) {
            ackMessageService.addMessage(message, consumer);
        }
    }
}
