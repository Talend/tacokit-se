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
package org.talend.components.marketo.service;

import static org.junit.Assert.*;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class AuthorizationClientTest extends MarketoBaseTest {

    @Service
    AuthorizationClient authClient;

    JsonObject json_602;

    JsonObject json_608;

    JsonObject json_1003;

    JsonArray json_errors;

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        json_602 = jsonFactory.createObjectBuilder().add("code", "602").add("message", "Invalid client Id").build();
        json_608 = jsonFactory.createObjectBuilder().add("code", "608").add("message", "API Temporarily Unavailable").build();
        json_1003 = jsonFactory.createObjectBuilder().add("code", "1003").add("message", "Non recoverable error").build();
    }

    @Test
    void isAccessTokenExpired() {
        json_errors = jsonFactory.createArrayBuilder().add(json_602).build();
        assertTrue(authClient.isAccessTokenExpired(json_errors));
        json_errors = jsonFactory.createArrayBuilder().add(json_1003).build();
        assertFalse(authClient.isAccessTokenExpired(json_errors));
        assertFalse(authClient.isAccessTokenExpired(null));
    }

    @Test
    void isErrorRecoverable() {
        json_errors = jsonFactory.createArrayBuilder().add(json_608).build();
        assertTrue(authClient.isErrorRecoverable(json_errors));
        json_errors = jsonFactory.createArrayBuilder().add(json_608).add(json_1003).build();
        assertTrue(authClient.isErrorRecoverable(json_errors));
        json_errors = jsonFactory.createArrayBuilder().add(json_1003).build();
        assertFalse(authClient.isErrorRecoverable(json_errors));
    }

}
