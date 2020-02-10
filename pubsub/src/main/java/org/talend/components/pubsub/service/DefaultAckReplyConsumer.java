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
package org.talend.components.pubsub.service;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import org.talend.components.pubsub.datastore.PubSubDataStore;

import java.io.Serializable;

public class DefaultAckReplyConsumer implements AckReplyConsumer, Serializable {

    private AckMessageService ackMessageService;

    private SubscriberStub subscriberStub;

    private PubSubDataStore dataStore;

    private String topic;

    private String subscriptionId;

    private String ackId;

    public static class Builder {

        private final DefaultAckReplyConsumer bean = new DefaultAckReplyConsumer();

        public DefaultAckReplyConsumer build() {
            return bean;
        }

        public Builder setAckMessageService(AckMessageService ackMessageService) {
            bean.ackMessageService = ackMessageService;
            return this;
        }

        public Builder setSubscriberStub(SubscriberStub subscriberStub) {
            bean.subscriberStub = subscriberStub;
            return this;
        }

        public Builder setDataStore(PubSubDataStore dataStore) {
            bean.dataStore = dataStore;
            return this;
        }

        public Builder setTopic(String topic) {
            bean.topic = topic;
            return this;
        }

        public Builder setSubscriptionId(String subscriptionId) {
            bean.subscriptionId = subscriptionId;
            return this;
        }

        public Builder setAckId(String ackId) {
            bean.ackId = ackId;
            return this;
        }
    }

    private DefaultAckReplyConsumer() {

    }

    @Override
    public void ack() {
        ackMessageService.ackMessage(subscriberStub, dataStore, topic, subscriptionId, ackId);
    }

    @Override
    public void nack() {

    }
}
