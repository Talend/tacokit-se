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
package org.talend.components.workday.dataset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class SwaggerLoaderTest {

    @Test
    void findGetServices() {
        final Map<String, List<WorkdayDataSet.Parameter>> services = new SwaggerLoader().findGetServices();

        Assertions.assertNotNull(services);

        Assertions.assertFalse(services.isEmpty());

        final Map.Entry<String, List<WorkdayDataSet.Parameter>> service = services.entrySet().iterator().next();

        Assertions.assertFalse(service.getValue().isEmpty());

    }
}