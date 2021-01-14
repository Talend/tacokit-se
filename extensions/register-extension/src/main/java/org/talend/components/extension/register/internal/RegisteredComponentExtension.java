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
package org.talend.components.extension.register.internal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.extension.register.api.CustomComponentExtension;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.container.ContainerManager;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.spi.ContainerListenerExtension;
import org.talend.sdk.component.spi.component.ComponentExtension;

import javax.annotation.Priority;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@Slf4j
public class RegisteredComponentExtension implements ContainerListenerExtension {

    private static ComponentManager componentManager; // workaround ComponentExtension

    private static RegisteredComponentExtension selfAsListener; // workaround ComponentExtension

    public RegisteredComponentExtension() {
        selfAsListener = this;
    }

    // @Override 1.1.17 API
    public int order() {
        return Integer.getInteger(getClass().getName() + ".order", 10);
    }

    @Override
    public void onCreate(final Container container) {
        log.info("RegisteredComponentExtension extension will load CustomComponentExtension...");
        final List<CustomComponentExtension> extensions = loadExtensions(container);
        container.set(Closeables.class, new Closeables(extensions.stream().map(it -> it.onCreate(container))
                .filter(Optional::isPresent).flatMap(Optional::get).collect(toList())));
    }

    @Override
    public void onClose(final Container container) {
        final IllegalStateException ise = new IllegalStateException("Invalid undeployment");
        ofNullable(container.get(Closeables.class)).ifPresent(c -> c.tasks.forEach(it -> {
            try {
                it.run();
            } catch (final RuntimeException re) {
                ise.addSuppressed(re);
            }
        }));
        if (ise.getSuppressed().length > 0) {
            throw ise;
        }
    }

    private List<CustomComponentExtension> loadExtensions(final Container container) {
        final List<CustomComponentExtension> extensions = StreamSupport
                .stream(ServiceLoader.load(CustomComponentExtension.class, container.getLoader()).spliterator(), false)
                .sorted(comparing(
                        it -> ofNullable(it.getClass().getAnnotation(Priority.class)).map(Priority::value).orElse(1000)))
                .collect(toList());

        extensions.forEach(c -> log.info("New loaded CustomComponentExtension : " + c.getClass().getName()));

        return extensions;
    }

    @Override // workaround ComponentExtension
    public void setComponentManager(final ComponentManager manager) {
        componentManager = manager;
    }

    /* ComponentExtension workaround to ensure the order before 1.1.17 release is used */
    public static class ExtensionOrderingWorkaround implements ComponentExtension {

        public ExtensionOrderingWorkaround() {
            if (selfAsListener == null) {
                return;
            }
            final ContainerManager container = componentManager.getContainer();
            container.unregisterListener(selfAsListener);
            container.registerListener(selfAsListener);
            selfAsListener = null;
            componentManager = null;
        }

        @Override // workaround ComponentExtension
        public synchronized void onComponent(final ComponentContext context) {
            // no-op
        }

        @Override // workaround ComponentExtension, always disabled
        public boolean supports(final Class<?> componentType) {
            return false;
        }

        @Override // workaround ComponentExtension, never called
        public <T> T convert(final ComponentInstance instance, final Class<T> component) {
            throw new UnsupportedOperationException("shouldn't be called cause supports returns false");
        }
    }

    public static class ClassloadingCustomizer implements ComponentManager.Customizer {

        @Override
        public Stream<String> containerClassesAndPackages() {
            return Stream.of("org.talend.components.extension.register.api", "org.talend.sdk.component.container",
                    "org.talend.sdk.component.design.");
        }
    }

    @RequiredArgsConstructor
    private static class Closeables {

        private final Collection<Runnable> tasks;
    }
}
