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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collections;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.talend.components.pubsub.BasePubSubTest;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

@Slf4j
@WithComponents("org.talend.components.pubsub")
class UIActionServiceTest extends BasePubSubTest {

    @Test
    void validateDataStore() {
        assertEquals(HealthCheckStatus.Status.OK, getUiActionService().validateDataStore(getDataStore()).getStatus());
    }

    @Test
    void validateDataStoreKO() {
        assertEquals(HealthCheckStatus.Status.KO, getUiActionService().validateDataStore(getInvalidDataStore()).getStatus());
    }

    @Test
    void listTopics() {
        SuggestionValues topics = getUiActionService().listTopics(getDataStore());
        assertNotNull(topics);
        assertThat(topics.getItems(), Matchers.contains(new SuggestionValues.Item(TOPIC, TOPIC)));
    }

    @Test
    void listTopicsKO() {
        assertEquals(Collections.emptyList(), getUiActionService().listTopics(getInvalidDataStore()).getItems());
    }

    @Test
    void listSubscriptions() {
        SuggestionValues subscriptions = getUiActionService().listSubscriptions(getDataStore());
        assertNotNull(subscriptions);
        assertThat(subscriptions.getItems(), Matchers.contains(new SuggestionValues.Item(SUBSCRIPTION, SUBSCRIPTION)));
    }

    @Test
    void listSubscriptionsKO() {
        assertEquals(Collections.emptyList(), getUiActionService().listSubscriptions(getInvalidDataStore()).getItems());
    }

}
