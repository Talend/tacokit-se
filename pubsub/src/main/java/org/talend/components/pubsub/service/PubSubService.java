package org.talend.components.pubsub.service;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;

import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient.ListSubscriptionsPagedResponse;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient.ListTopicsPagedResponse;
import com.google.cloud.pubsub.v1.TopicAdminSettings;

@Slf4j
@Service
public class PubSubService {

    @Service
    private I18nMessage i18n;

    public Credentials getCredentials(PubSubDataStore dataStore) {
        try {
            return GoogleCredentials.fromStream(new FileInputStream(dataStore.getServiceAccountFile()));
        } catch (IOException e) {
            throw new RuntimeException(i18n.errorGetCredentials(dataStore.getServiceAccountFile(), e.getMessage()));
        }
    }

    public ListTopicsPagedResponse listTopics(@Option final PubSubDataStore dataStore) throws IOException {
        TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(getCredentials(dataStore))).build();
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
            return topicAdminClient.listTopics(dataStore.getProjectName());
        }
    }

    public ListSubscriptionsPagedResponse listSubscriptions(@Option final PubSubDataStore dataStore) throws IOException {
        SubscriptionAdminSettings settings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(getCredentials(dataStore))).build();
        try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(settings)) {
            return admin.listSubscriptions(dataStore.getProjectName());
        }
    }

    public String extractTopicOrSubscription(String path) {
        return path.split("/")[3];
    }

}
