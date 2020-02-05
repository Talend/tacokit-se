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

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.talend.components.extension.api.veto.ExcludedIf;
import org.talend.components.extension.api.veto.IncludedIf;
import org.talend.components.extension.api.KeyValue;
import org.talend.components.extension.api.extension.CustomComponentExtension;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.base.Lifecycle;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ComponentManager;

public class ExcludedComponentExtension implements CustomComponentExtension {

    @Override
    public Optional<Stream<Runnable>> onCreate(final Container container) {
        return getFamilies(container).map(families -> {
            families.forEach(family -> vetoExcludedComponents(family,
                    ofNullable(container.get(ComponentManager.AllServices.class))
                            .map(a -> LocalConfiguration.class.cast(a.getServices().get(LocalConfiguration.class)))
                            .orElse(null)));
            return Stream.empty(); // no closing task for this processor
        });
    }

    private void vetoExcludedComponents(final ComponentFamilyMeta componentFamilyMeta, final LocalConfiguration configuration) {
        final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers = componentFamilyMeta.getPartitionMappers();
        final Map<String, ComponentFamilyMeta.ProcessorMeta> processors = componentFamilyMeta.getProcessors();
        Stream.concat(mappers.values().stream(), processors.values().stream())
                .filter(meta -> isExcluded(meta, configuration, mappers, processors)
                        || !isIncluded(meta, configuration, mappers, processors))
                .collect(toList()).forEach(meta -> {
                    if (ComponentFamilyMeta.PartitionMapperMeta.class.isInstance(meta)) {
                        mappers.remove(meta.getName());
                    } else {
                        processors.remove(meta.getName());
                    }
                });
    }

    private boolean isExcluded(final ComponentFamilyMeta.BaseMeta<? extends Lifecycle> meta,
            final LocalConfiguration configuration, final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers,
            final Map<String, ComponentFamilyMeta.ProcessorMeta> processors) {
        final ExcludedIf config = meta.getType().getAnnotation(ExcludedIf.class);
        return config != null && (isConfigurationSet(config.configuration(), configuration)
                || isComponentPresent(config.componentExistsInTheFamily(), mappers, processors));
    }

    private boolean isIncluded(final ComponentFamilyMeta.BaseMeta<? extends Lifecycle> meta,
            final LocalConfiguration configuration, final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers,
            final Map<String, ComponentFamilyMeta.ProcessorMeta> processors) {
        final IncludedIf config = meta.getType().getAnnotation(IncludedIf.class);
        return config == null || isConfigurationSet(config.configuration(), configuration)
                || isComponentPresent(config.componentExistsInTheFamily(), mappers, processors);
    }

    private boolean isComponentPresent(final String[] componentExistsInTheFamily,
            final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers,
            final Map<String, ComponentFamilyMeta.ProcessorMeta> processors) {
        return Stream.of(componentExistsInTheFamily).anyMatch(name -> mappers.containsKey(name) || processors.containsKey(name));
    }

    private boolean isConfigurationSet(final KeyValue[] pairs, final LocalConfiguration configuration) {
        return configuration != null && Stream.of(pairs).anyMatch(kv -> Objects.equals(kv.value(), configuration.get(kv.name())));
    }
}
