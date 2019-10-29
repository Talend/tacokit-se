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
import org.talend.sdk.component.api.service.completion.Values;

import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class SwaggerLoaderTest {

    @Test
    void findGetServices() {

        final URL swaggersDirectory = Thread.currentThread().getContextClassLoader().getResource("swaggers/");
        SwaggerLoader loader = new SwaggerLoader(swaggersDirectory.getPath());
        final Collection<Values.Item> modules = loader.getModules();
        Assertions.assertNotNull(modules);
        Assertions.assertFalse(modules.isEmpty());

        Values.Item item = modules.iterator().next();
        Assertions.assertNotNull(item, "item null");
        Assertions.assertNotNull(item.getId(), "item id null");
        Assertions.assertNotNull(item.getLabel(), "item label null");

        final Map<String, List<WorkdayDataSet.Parameter>> services = loader.findGetServices(item.getId());
        Assertions.assertNotNull(services);
    }

    @Test
    void loaderByJar() {
        final URL swaggersInJars = Thread.currentThread().getContextClassLoader().getResource("test.jar");

        String path = swaggersInJars.getPath() + "!/swaggers/";
        SwaggerLoader loader = new SwaggerLoader(path);

        final Collection<Values.Item> modules = loader.getModules();
        Assertions.assertNotNull(modules);
        Assertions.assertFalse(modules.isEmpty());

        Values.Item item = modules.iterator().next();
        Assertions.assertNotNull(item);
        Assertions.assertEquals("swaggers/student-swagger.json", item.getId());
        Assertions.assertEquals("Student", item.getLabel());

        final Map<String, List<WorkdayDataSet.Parameter>> services = loader.findGetServices(item.getId());
        Assertions.assertNotNull(services);
    }

}