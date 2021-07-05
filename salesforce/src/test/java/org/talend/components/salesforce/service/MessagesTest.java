/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.salesforce.service;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.salesforce")
class MessagesTest {

    @Service
    private Messages messages;

    @Test
    void testMsg() {
        Assertions.assertNotNull(messages.warnBatchTimeout());
        Assertions.assertNotNull(messages.healthCheckOk());

        final String healthCheckFailed = messages.healthCheckFailed("cause");
        Assertions.assertNotNull(healthCheckFailed);
        Assertions.assertTrue(healthCheckFailed.contains("cause"));

        Assertions.assertNotNull(messages.errorPasswordExpired());

        final String failedPipeline = messages.failedPipeline("operationName", "errorData");
        Assertions.assertNotNull(failedPipeline);
        Assertions.assertTrue(failedPipeline.contains("operationName"));
        Assertions.assertTrue(failedPipeline.contains("errorData"));
    }
}