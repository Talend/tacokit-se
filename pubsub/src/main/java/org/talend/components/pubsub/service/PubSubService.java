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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

@Slf4j
@Service
public class PubSubService {

    public static final String ACTION_HEALTH_CHECK = "HEALTH_CHECK";

    public static final String ACTION_SUGGESTION_TOPICS = "SUGGESTION_TOPICS";

    public static final String ACTION_SUGGESTION_SUBSCRIPTIONS = "SUGGESTION_SUBSCRIPTIONS";

    @Service
    private I18nMessage i18n;

    @HealthCheck(ACTION_HEALTH_CHECK)
    public HealthCheckStatus validateDataStore(@Option final PubSubDataStore dataStore) {

        if (dataStore.getProjectName() == null || "".equals(dataStore.getProjectName().trim())) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.projectNameRequired());
        }
        if (dataStore.getJsonCredentials() == null || "".equals(dataStore.getJsonCredentials().trim())) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.credentialsRequired());
        }

        try (TopicAdminClient topicAdminClient = TopicAdminClient
                .create(TopicAdminSettings.newBuilder().setCredentialsProvider(() -> createCredentials(dataStore)).build())) {

        } catch (final Exception e) {
            log.error("[HealthCheckStatus] {}", e.getMessage());
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
    }

    @Suggestions(ACTION_SUGGESTION_TOPICS)
    public SuggestionValues listTopics(@Option final PubSubDataStore dataStore) {
        try (TopicAdminClient topicAdminClient = TopicAdminClient
                .create(TopicAdminSettings.newBuilder().setCredentialsProvider(() -> createCredentials(dataStore)).build())) {

            TopicAdminClient.ListTopicsPagedResponse response = topicAdminClient.listTopics(
                    ListTopicsRequest.newBuilder().setProject(ProjectName.format(dataStore.getProjectName())).build());
            Iterable<Topic> topics = response.iterateAll();

            return new SuggestionValues(true,
                    StreamSupport.stream(topics.spliterator(), false)
                            .map(topic -> ProjectTopicName.parse(topic.getName()).getTopic())
                            .map(topicName -> new SuggestionValues.Item(topicName, topicName)).collect(toList()));
        } catch (final Exception unexpected) {
            log.error(i18n.errorListTopics(unexpected.getMessage()));
        }

        return new SuggestionValues(false, emptyList());
    }

    @Suggestions(ACTION_SUGGESTION_SUBSCRIPTIONS)
    public SuggestionValues listSubscriptions(@Option final PubSubDataStore dataStore, final String topic) {
        try (TopicAdminClient topicAdminClient = TopicAdminClient
                .create(TopicAdminSettings.newBuilder().setCredentialsProvider(() -> createCredentials(dataStore)).build())) {
            ProjectTopicName topicName = ProjectTopicName.of(dataStore.getProjectName(), topic);
            TopicAdminClient.ListTopicSubscriptionsPagedResponse response = topicAdminClient
                    .listTopicSubscriptions(ListTopicSubscriptionsRequest.newBuilder().setTopic(topicName.toString()).build());
            Iterable<String> subscriptions = response.iterateAll();

            return new SuggestionValues(true,
                    StreamSupport.stream(subscriptions.spliterator(), false)
                            .map(sub -> ProjectSubscriptionName.parse(sub).getSubscription())
                            .map(sub -> new SuggestionValues.Item(sub, sub)).collect(toList()));

        } catch (final Exception unexpected) {
            log.error(i18n.errorListSubscriptions(unexpected.getMessage()));
        }
        return new SuggestionValues(false, emptyList());
    }

    private GoogleCredentials createCredentials(PubSubDataStore dataStore) {
        GoogleCredentials credentials = null;
        if (dataStore.getJsonCredentials() != null && !"".equals(dataStore.getJsonCredentials().trim())) {
            credentials = getCredentials(dataStore.getJsonCredentials());
        }

        return credentials;
    }

    public Subscriber createSubscriber(PubSubDataStore dataStore, String topic, String subscriptionId, MessageReceiver receiver) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(dataStore.getProjectName(), subscriptionId);
        createSubscriptionIfNeeded(dataStore, topic, subscriptionName);
        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                .setCredentialsProvider(() -> createCredentials(dataStore)).build();
        return subscriber;
    }

    public SubscriberStub createSubscriber(PubSubDataStore dataStore, String topic, String subscriptionId) {
        try {
            ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(dataStore.getProjectName(), subscriptionId);
            createSubscriptionIfNeeded(dataStore, topic, subscriptionName);
            SubscriberStub subscriber = GrpcSubscriberStub.create(
                    SubscriberStubSettings.newBuilder().setCredentialsProvider(() -> createCredentials(dataStore)).build());
            return subscriber;
        } catch (IOException ioe) {
            log.error(i18n.errorCreateSubscriber(ioe.getMessage()));
            throw new PubSubConnectorException(i18n.errorCreateSubscriber(ioe.getMessage()), ioe);
        }
    }

    /**
     * Checks if the subscription exists and create it if not
     * 
     * @param dataStore
     * @param subscriptionName
     */
    private void createSubscriptionIfNeeded(PubSubDataStore dataStore, String topic, ProjectSubscriptionName subscriptionName) {
        try {
            SubscriptionAdminSettings adminSettings = SubscriptionAdminSettings.newBuilder()
                    .setCredentialsProvider(() -> createCredentials(dataStore)).build();
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(adminSettings)) {
                try {
                    Subscription subscription = subscriptionAdminClient.getSubscription(subscriptionName);
                    log.info(i18n.subscriptionFound(subscription.toString()));
                } catch (ApiException apiEx) {
                    log.info(i18n.subscriptionNotFound());
                    ProjectTopicName topicName = ProjectTopicName.of(dataStore.getProjectName(), topic);
                    Subscription subscription = subscriptionAdminClient.createSubscription(subscriptionName, topicName,
                            PushConfig.getDefaultInstance(), 10);
                    log.info(i18n.subscriptionCreated(subscription.toString()));
                }
            }
        } catch (Exception e) {
            log.warn(i18n.errorCreateSubscription(e.getMessage()));
        }
    }

    public void removeSubscription(PubSubDataStore dataStore, String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(dataStore.getProjectName(), subscriptionId);
        try {
            SubscriptionAdminSettings adminSettings = SubscriptionAdminSettings.newBuilder()
                    .setCredentialsProvider(() -> createCredentials(dataStore)).build();
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(adminSettings)) {
                subscriptionAdminClient.deleteSubscription(subscriptionName);
                log.info(i18n.subscriptionDeleted(subscriptionId));
            } catch (ApiException apiEx) {
                log.error(i18n.cannotDeleteSubscription(apiEx.getMessage()), apiEx);
            }
        } catch (Exception e) {
            log.warn(i18n.errorRemoveSubscription(e.getMessage()));
        }
    }

    public GoogleCredentials getCredentials(String credentials) {
        try {
            return GoogleCredentials.fromStream(new ByteArrayInputStream(credentials.getBytes()));
        } catch (IOException e) {
            log.error(i18n.errorCredentials(e.getMessage()), e);
            throw new PubSubConnectorException(i18n.errorCredentials(e.getMessage()));
        }
    }

    public Publisher createPublisher(PubSubDataStore dataStore, String topic) {
        try {
            return Publisher.newBuilder(ProjectTopicName.of(dataStore.getProjectName(), topic))
                    .setCredentialsProvider(() -> createCredentials(dataStore)).build();
        } catch (IOException e) {
            log.error(i18n.errorCreatePublisher(e.getMessage()), e);
            throw new PubSubConnectorException(i18n.errorCreatePublisher(e.getMessage()));
        }
    }

}
