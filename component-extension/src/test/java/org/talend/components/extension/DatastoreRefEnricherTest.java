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
package org.talend.components.extension;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ContainerComponentRegistry;
import org.talend.sdk.component.runtime.manager.ParameterMeta;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.junit.jupiter.api.Assertions.*;

@WithComponents(value = "org.talend.components.extension.components")
class DatastoreRefEnricherTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Test
    void validDatastoreRef() {
        final Container plugin = componentsHandler.asManager().findPlugin("test-classes")
                .orElseThrow(() -> new IllegalStateException("test plugin can't be found"));
        assertNotNull(plugin);

        ComponentFamilyMeta family = plugin.get(ContainerComponentRegistry.class).getComponents().get("Test");
        Map<String, ParameterMeta> ref = getDatastoreRef(family, "Valid");

        assertEquals(ref.size(), 2);

        // datastore1 field
        assertTrue(Optional.ofNullable(ref.get("datastore1")).isPresent());
        ParameterMeta datastore1Meta = ref.get("datastore1");
        assertEquals("datastore1Conf", datastore1Meta.getMetadata().get("connectors::extensions::configurationtyperef::id"));
        assertEquals("datastore", datastore1Meta.getMetadata().get("connectors::extensions::configurationtyperef::type"));

        // filters
        assertEquals("type", datastore1Meta.getMetadata().get("connectors::extensions::configurationtyperef::filter[0].key"));
        assertEquals("Oauth1", datastore1Meta.getMetadata().get("connectors::extensions::configurationtyperef::filter[0].value"));

        assertEquals("type", datastore1Meta.getMetadata().get("connectors::extensions::configurationtyperef::filter[1].key"));
        assertEquals("Oauth2", datastore1Meta.getMetadata().get("connectors::extensions::configurationtyperef::filter[1].value"));

        // datastore2 field
        assertTrue(Optional.ofNullable(ref.get("datastore2")).isPresent());
        ParameterMeta datastore2Meta = ref.get("datastore2");
        assertEquals("datastore2Conf", datastore2Meta.getMetadata().get("connectors::extensions::configurationtyperef::id"));
        assertEquals("datastore", datastore2Meta.getMetadata().get("connectors::extensions::configurationtyperef::type"));
        assertNull(datastore2Meta.getMetadata().get("tcomp::configurationtyperef::filter[0].key"));
    }

    private Map<String, ParameterMeta> getDatastoreRef(ComponentFamilyMeta family, String component) {
        return family.getPartitionMappers().get(component).getParameterMetas().get().stream().flatMap(this::flatten)
                .filter(p -> p.getMetadata().containsKey("connectors::extensions::configurationtyperef::type"))
                .collect(Collectors.toMap(ParameterMeta::getName, Function.identity()));
    }

    private Stream<ParameterMeta> flatten(final ParameterMeta meta) {
        return concat(of(meta), meta.getNestedParameters().stream().flatMap(this::flatten));
    }

}
