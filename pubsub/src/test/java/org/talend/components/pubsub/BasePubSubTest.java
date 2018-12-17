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
package org.talend.components.pubsub;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.BeforeEach;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.components.pubsub.service.PubSubService;
import org.talend.components.pubsub.service.UIActionService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

@Data
@Slf4j
@WithComponents("org.talend.components.pubsub")
public abstract class BasePubSubTest {

    public static final String PROJECT = "projects/talend-pubsub";

    public static final String TOPIC = "tacokit";

    public static final String SUBSCRIPTION = "tacokit-sub";

    public static final String PATH_TOPIC = PROJECT + "/topics/" + TOPIC;

    public static final String SERVICE_ACCOUNT_FILE = "/home/undx/tacokit-pubsub.json";

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private PubSubService service;

    @Service
    private UIActionService uiActionService;

    @Service
    private I18nMessage i18n;

    private PubSubDataStore dataStore;

    private PubSubDataStore invalidDataStore;

    private PubSubDataSet dataSet;

    private PubSubDataSet invalidDataSet;

    @BeforeEach
    void setUp() {
        dataStore = new PubSubDataStore();
        dataStore.setProjectName(PROJECT);
        dataStore.setServiceAccountFile(SERVICE_ACCOUNT_FILE);
        dataSet = new PubSubDataSet();
        dataSet.setDataStore(dataStore);
    }
}
