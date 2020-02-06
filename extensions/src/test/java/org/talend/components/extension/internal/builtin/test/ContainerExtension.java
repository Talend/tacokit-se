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
package org.talend.components.extension.internal.builtin.test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.container.ContainerManager;
import org.talend.sdk.component.dependencies.maven.Artifact;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.ContainerComponentRegistry;

public class ContainerExtension implements ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(ContainerExtension.class);

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        final Class<?> type = parameterContext.getParameter().getType();
        return Container.class == type || ComponentFamilyMeta.class == type || ContainerComponentRegistry.class == type;
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        final Class<?> type = parameterContext.getParameter().getType();
        if (Container.class == type) {
            return getContainer(extensionContext);
        }
        if (ComponentFamilyMeta.class == type) {
            return getContainer(extensionContext).get(ContainerComponentRegistry.class).getComponents().values().iterator()
                    .next();
        }
        if (ContainerComponentRegistry.class == type) {
            return getContainer(extensionContext).get(ContainerComponentRegistry.class);
        }
        throw new ParameterResolutionException("Unsupported parameter: " + parameterContext);
    }

    private Container getContainer(final ExtensionContext extensionContext) {
        return extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent(Container.class, k -> {
            final ComponentFamilyMeta family = new ComponentFamilyMeta("test", singleton("TestCategory"), "test-icon",
                    "TestFamily", "org.fake.test");

            final ContainerComponentRegistry instance = new ContainerComponentRegistry();
            instance.getComponents().put("TestFamily", family);

            final Container container = new Container(
                    "test", "whatever", new Artifact[0], ContainerManager.ClassLoaderConfiguration.builder()
                            .parent(Thread.currentThread().getContextClassLoader()).create(),
                    gav -> Paths.get("target/fake/" + gav), c -> {
                    }, new String[0], false);
            container.set(ComponentManager.AllServices.class,
                    new ComponentManager.AllServices(singletonMap(LocalConfiguration.class, new LocalConfiguration() {

                        @Override
                        public String get(final String key) {
                            return "conf-set".equals(key) ? "valued" : null;
                        }

                        @Override
                        public Set<String> keys() {
                            return singleton("conf-set");
                        }
                    })));
            container.set(ContainerComponentRegistry.class, instance);
            return container;
        }, Container.class);
    }

    public ComponentFamilyMeta.PartitionMapperMeta newMapper(final Class<?> type, final ComponentFamilyMeta family) {
        return new ComponentFamilyMeta.PartitionMapperMeta(family, type.getSimpleName(), null, 1, type, Collections::emptyList,
                c -> null, () -> null, true, false) {
        };
    }

    public ComponentFamilyMeta.ProcessorMeta newProcessor(final Class<?> type, final ComponentFamilyMeta family) {
        return new ComponentFamilyMeta.ProcessorMeta(family, type.getSimpleName(), null, 1, type, Collections::emptyList,
                c -> null, () -> null, true) {
        };
    }
}
