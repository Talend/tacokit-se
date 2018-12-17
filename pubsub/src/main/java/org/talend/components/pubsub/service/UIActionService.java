// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.pubsub.service;

import lombok.extern.slf4j.Slf4j;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;

import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient.ListSubscriptionsPagedResponse;
import com.google.cloud.pubsub.v1.TopicAdminClient.ListTopicsPagedResponse;

@Slf4j
@Service
public class UIActionService {

    public static final String ACTION_HEALTH_CHECK = "HEALTH_CHECK";

    public static final String ACTION_SUGGESTION_SUBSCRIPTIONS = "SUBSCRIPTIONS";

    public static final String ACTION_SUGGESTION_TOPICS = "TOPICS";

    @Service
    private PubSubService service;

    @Service
    private I18nMessage i18n;

    @HealthCheck(ACTION_HEALTH_CHECK)
    public HealthCheckStatus validateDataStore(@Option final PubSubDataStore dataStore) {
        try {
            service.listTopics(dataStore);
        } catch (final Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
    }

    @Suggestions(ACTION_SUGGESTION_TOPICS)
    public SuggestionValues listTopics(@Option final PubSubDataStore dataStore) {
        try {
            ListTopicsPagedResponse topics = service.listTopics(dataStore);
            List<SuggestionValues.Item> values = new ArrayList<>();
            topics.iterateAll().forEach(topic -> {
                log.debug(topic.getName());
                String t = service.extractTopicOrSubscription(topic.getName());
                values.add(new SuggestionValues.Item(t, t));
            });
            return new SuggestionValues(true, values);
        } catch (final Exception unexpected) {
            log.error(i18n.errorListTopics(), unexpected);
        }
        return new SuggestionValues(false, emptyList());
    }

    @Suggestions(ACTION_SUGGESTION_SUBSCRIPTIONS)
    public SuggestionValues listSubscriptions(@Option final PubSubDataStore dataStore) {
        try {
            ListSubscriptionsPagedResponse subscriptions = service.listSubscriptions(dataStore);
            List<SuggestionValues.Item> values = new ArrayList<>();
            subscriptions.iterateAll().forEach(subscription -> {
                log.debug(subscription.getName());
                String t = service.extractTopicOrSubscription(subscription.getName());
                values.add(new SuggestionValues.Item(t, t));
            });
            return new SuggestionValues(true, values);
        } catch (final Exception unexpected) {
            log.error(i18n.errorListSubscriptions(), unexpected);
        }
        return new SuggestionValues(false, emptyList());
    }

}
