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
package org.talend.components.extension.internal.builtin;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.talend.components.extension.internal.builtin.test.ContainerExtension;
import org.talend.components.extension.internal.builtin.test.component.veto.Mapper1;
import org.talend.components.extension.internal.builtin.test.component.veto.Mapper2;
import org.talend.components.extension.internal.builtin.test.component.veto.Processor1;
import org.talend.components.extension.internal.builtin.test.component.veto.Processor2;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;

class ExcludedComponentExtensionTest {

    private final ExcludedComponentExtension excludedComponentExtension = new ExcludedComponentExtension();

    @RegisterExtension
    public final ContainerExtension containerExtension = new ContainerExtension();

    @Test
    void vetoWithConfiguration(final Container container, final ComponentFamilyMeta family) {
        family.getPartitionMappers().put("Mapper1", containerExtension.newMapper(Mapper1.class, family));
        family.getPartitionMappers().put("Mapper2", containerExtension.newMapper(Mapper2.class, family));
        assertEquals(0L, excludedComponentExtension.onCreate(container).map(Stream::count).orElse(-1L), 0);
        assertEquals(1, family.getPartitionMappers().size() + family.getProcessors().size());
        assertEquals("Mapper1", family.getPartitionMappers().keySet().iterator().next());
    }

    @Test
    void vetoWithOtherComponentPresence(final Container container, final ComponentFamilyMeta family) {
        family.getPartitionMappers().put("Mapper1", containerExtension.newMapper(Mapper1.class, family));
        family.getProcessors().put("Processor1", containerExtension.newProcessor(Processor1.class, family));
        assertEquals(0L, excludedComponentExtension.onCreate(container).map(Stream::count).orElse(-1L), 0);
        assertEquals(1, family.getPartitionMappers().size() + family.getProcessors().size());
        assertEquals("Mapper1", family.getPartitionMappers().keySet().iterator().next());
    }

    @Test
    void notVetoWithOtherComponentPresenceMissing(final Container container, final ComponentFamilyMeta family) {
        family.getProcessors().put("Processor1", containerExtension.newProcessor(Processor1.class, family));
        assertEquals(0L, excludedComponentExtension.onCreate(container).map(Stream::count).orElse(-1L), 0);
        assertEquals(1, family.getPartitionMappers().size() + family.getProcessors().size());
        assertEquals("Processor1", family.getProcessors().keySet().iterator().next());
    }

    @Test
    void includeIfOtherComponentIsMissing(final Container container, final ComponentFamilyMeta family) {
        family.getProcessors().put("Processor2", containerExtension.newProcessor(Processor2.class, family));
        assertEquals(0L, excludedComponentExtension.onCreate(container).map(Stream::count).orElse(-1L), 0);
        assertEquals(0, family.getPartitionMappers().size() + family.getProcessors().size());
    }

    @Test
    void includeIfOtherComponentIsPresent(final Container container, final ComponentFamilyMeta family) {
        family.getPartitionMappers().put("Mapper1", containerExtension.newMapper(Mapper1.class, family));
        family.getProcessors().put("Processor2", containerExtension.newProcessor(Processor2.class, family));
        assertEquals(0L, excludedComponentExtension.onCreate(container).map(Stream::count).orElse(-1L), 0);
        assertEquals(2, family.getPartitionMappers().size() + family.getProcessors().size());
        assertEquals(singleton("Mapper1"), family.getPartitionMappers().keySet());
        assertEquals(singleton("Processor2"), family.getProcessors().keySet());
    }
}
