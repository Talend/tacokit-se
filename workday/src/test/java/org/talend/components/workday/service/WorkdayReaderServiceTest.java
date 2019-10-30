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
package org.talend.components.workday.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.dataset.WorkdayServiceDataSet;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Values;

import java.util.Collection;

class WorkdayReaderServiceTest {

    @Test
    void loadModules() {
        WorkdayReaderService reader = new WorkdayReaderService();
        final Values modules = reader.loadModules();

        Assertions.assertNotNull(modules);
        final Collection<Values.Item> modulesItems = modules.getItems();
        Assertions.assertNotNull(modulesItems);
        Assertions.assertFalse(modulesItems.isEmpty());

        final Values.Item module = modulesItems.iterator().next();

        final SuggestionValues services = reader.loadServices(module.getId());
        Assertions.assertNotNull(services);
        final Collection<SuggestionValues.Item> servicesItems = services.getItems();
        Assertions.assertNotNull(servicesItems);
        Assertions.assertFalse(servicesItems.isEmpty());

        final SuggestionValues.Item service = servicesItems.iterator().next();
        Assertions.assertNotNull(service);

        final WorkdayServiceDataSet.Parameters parameters = reader.loadServiceParameter(module.getId(), service.getId());
        Assertions.assertNotNull(parameters);

    }
}