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
package org.talend.components.pubsub.service;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.sdk.component.api.service.Service;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AckMessageService implements Serializable {

    @Service
    private PubSubService pubSubService;

    private final Map<String, AckReplyConsumer> messagesToAck = new ConcurrentHashMap<>();

    public boolean messageExists(String messageId) {
        return messagesToAck.containsKey(messageId);
    }

    public void removeMessage(String messageId) {
        messagesToAck.remove(messageId);
    }

    public void ackMessage(String messageId) {
        AckReplyConsumer consumer = messagesToAck.remove(messageId);
        if (consumer != null) {
            consumer.ack();
        }
    }

    protected void ackMessage(SubscriberStub subscriberStub, PubSubDataStore dataStore, String topic, String subscriptionId,
            String ackId) {
        if (subscriberStub == null || subscriberStub.isShutdown()) {
            subscriberStub = pubSubService.createSubscriber(dataStore, topic, subscriptionId);
        }

        AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
                .setSubscription(ProjectSubscriptionName.format(dataStore.getProjectName(), subscriptionId))
                .addAllAckIds(Collections.singleton(ackId)).build();
        subscriberStub.acknowledgeCallable().call(acknowledgeRequest);
    }

    public void addMessage(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
        messagesToAck.put(message.getMessageId(), ackReplyConsumer);
    }
}
