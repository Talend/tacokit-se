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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.talend.components.extension.api.extension.CustomComponentExtension;
import org.talend.components.extension.api.virtual.VirtualChain;
import org.talend.components.extension.internal.builtin.virtual.VirtualMapper;
import org.talend.sdk.component.api.component.MigrationHandler;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ParameterMeta;
import org.talend.sdk.component.runtime.manager.util.Lazy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * IMPORTANT: this is not handled by the framework so validations must be coded in a ComponentValidator extension!!!
 */
@Slf4j
public class VirtualComponentExtension implements CustomComponentExtension {

    @Override
    public Optional<Stream<Runnable>> onCreate(final Container container) {
        return getFamilies(container).map(families -> families.flatMap(family -> {
            processFamily(family);
            return Stream.empty(); // no clean up task here
        }));
    }

    private void processFamily(final ComponentFamilyMeta family) {
        final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers = family.getPartitionMappers();
        final List<MapperWithChains> mapperWithChains = findMapperWithChains(mappers);
        if (mapperWithChains.isEmpty()) {
            return;
        }

        // map configuration to partitionmappers
        final List<ComponentFamilyMeta.PartitionMapperMeta> newMappers = mapperWithChains.stream()
                .flatMap(mwc -> Stream.of(mwc.chains).map(it -> new MapperWithChain(mwc.mapperMeta, it)))
                .map(it -> toPartitionMapperMeta(it, family.getProcessors().values())).collect(toList());

        // now add all new partition mapper
        final Map<String, ComponentFamilyMeta.PartitionMapperMeta> newMeta = newMappers.stream()
                .collect(toMap(ComponentFamilyMeta.BaseMeta::getName, identity()));
        log.info("Created {} virtual sources", newMeta.keySet());
        mappers.putAll(newMeta);

        // drop all the needed parents
        mapperWithChains.stream().filter(mwc -> Stream.of(mwc.chains).anyMatch(VirtualChain::requiresToVetoParent))
                .map(mwc -> mwc.mapperMeta.getName()).forEach(mappers::remove);
    }

    private ComponentFamilyMeta.PartitionMapperMeta toPartitionMapperMeta(final MapperWithChain mapperWithChain,
            final Collection<ComponentFamilyMeta.ProcessorMeta> processors) {
        final ComponentFamilyMeta.PartitionMapperMeta src = mapperWithChain.mapperMeta;
        final List<ComponentFamilyMeta.ProcessorMeta> processorChain = Stream.of(mapperWithChain.chain.followedBy())
                .map(procClass -> processors.stream().filter(it -> it.getType() == procClass).findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Processor '" + procClass.getName() + "' not found")))
                .collect(toList());
        final int version = mapperWithChain.mapperMeta.getVersion()
                + processors.stream().mapToInt(ComponentFamilyMeta.ProcessorMeta::getVersion).sum();
        return new ComponentFamilyMeta.PartitionMapperMeta(src.getParent(), mapperWithChain.chain.name(),
                mapperWithChain.chain.icon(), version, src.getType() /* not important */,
                createConfigurationSupplier(mapperWithChain.mapperMeta, processorChain),
                createInstantiator(mapperWithChain.chain.name(), mapperWithChain.mapperMeta, processorChain),
                createMigrationHandlerSupplier(version, mapperWithChain.mapperMeta, processorChain), true, false) {
        };
    }

    private Supplier<MigrationHandler> createMigrationHandlerSupplier(final int currentVersion,
            final ComponentFamilyMeta.PartitionMapperMeta mapperMeta,
            final List<ComponentFamilyMeta.ProcessorMeta> processorChain) {
        return Lazy.lazy(() -> (incomingVersion, incomingData) -> {
            if (incomingData == null || incomingVersion == currentVersion) {
                return incomingData;
            }

            // here we must split the incoming data per component, migrate it and re-prefix it
            final Map<String, Map<String, String>> confs = splitConfigurationPerSubComponent(incomingData);
            final Map<String, String> migratedConf = Stream.concat(Stream.of(mapperMeta), processorChain.stream())
                    .map(meta -> prefixWith("configuration." + meta.getName() + ".",
                            meta.getMigrationHandler().get().migrate(findStoreSubComponentVersion(incomingData, meta),
                                    confs.getOrDefault(meta.getName(), new HashMap<>()))))
                    .flatMap(map -> map.entrySet().stream()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            // passthrough internal configs ($ in keys)
            migratedConf.putAll(incomingData.entrySet().stream()
                    .filter(it -> !migratedConf.containsKey(it.getKey()) && it.getKey().contains("$"))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
            return migratedConf;
        });
    }

    private int findStoreSubComponentVersion(final Map<String, String> incomingData, final ComponentFamilyMeta.BaseMeta<?> meta) {
        try {
            return Integer.parseInt(incomingData.getOrDefault("configuration." + getVersionOptionName(meta), "0"));
        } catch (final NumberFormatException nfe) {
            log.warn("No version found for " + meta.getName());
            return 0;
        }
    }

    private List<MapperWithChains> findMapperWithChains(final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers) {
        return mappers.values().stream()
                .map(it -> new MapperWithChains(it, it.getType().getAnnotationsByType(VirtualChain.class)))
                .filter(it -> it.chains.length > 0).collect(toList());
    }

    private Function<Map<String, String>, Mapper> createInstantiator(final String name,
            final ComponentFamilyMeta.PartitionMapperMeta mapperMeta,
            final List<ComponentFamilyMeta.ProcessorMeta> processorChain) {
        return configuration -> {
            final Map<String, Map<String, String>> confs = splitConfigurationPerSubComponent(configuration);
            return new VirtualMapper(name,
                    mapperMeta.getInstantiator().apply(confs.getOrDefault(mapperMeta.getName(), emptyMap())),
                    processorChain.stream().map(p -> p.getInstantiator().apply(confs.getOrDefault(p.getName(), emptyMap())))
                            .collect(toList()));
        };
    }

    private Map<String, Map<String, String>> splitConfigurationPerSubComponent(final Map<String, String> configuration) {
        return configuration.entrySet().stream()
                .filter(it -> it.getKey().startsWith("configuration.") && findRealKeyIndex(it.getKey()) > 0)
                .collect(groupingBy(e -> e.getKey().substring("configuration.".length(), findRealKeyIndex(e.getKey())), // comp
                                                                                                                        // name
                        toMap(e -> e.getKey().substring(findRealKeyIndex(e.getKey()) + 1), Map.Entry::getValue)));
    }

    private int findRealKeyIndex(final String key) {
        return key.indexOf('.', "configuration.".length() + 1);
    }

    private Supplier<List<ParameterMeta>> createConfigurationSupplier(final ComponentFamilyMeta.PartitionMapperMeta mapperMeta,
            final List<ComponentFamilyMeta.ProcessorMeta> processorChain) {
        // here, until we get a layout in the @VirtualChain annotation, we will just stack the configurations of all components
        // prefixing it with the name of the component - it is likely unique
        // we then just wrap them all in a single object "configuration"
        return Lazy.lazy(() -> {
            final List<ParameterMeta> nestedParameters = Stream.of(
                    prefixWith("configuration." + mapperMeta.getName(), mapperMeta.getParameterMetas().get()),
                    processorChain.stream().flatMap(
                            procMeta -> prefixWith("configuration." + procMeta.getName(), procMeta.getParameterMetas().get())))
                    .flatMap(identity()).collect(toList());

            // now we add for each component a hidden configuration hosting (thanks to the default) the actual version
            // of the component to be able to impl the migration
            nestedParameters.addAll(Stream.concat(Stream.of(mapperMeta), processorChain.stream()).map(meta -> {
                final String name = getVersionOptionName(meta);
                final Map<String, String> metadata = new HashMap<>();
                metadata.put("extension::virtual::synthetic", "true");
                metadata.put("tcomp::ui::defaultvalue::value", Integer.toString(meta.getVersion()));
                return new ParameterMeta(null, int.class, ParameterMeta.Type.NUMBER, "configuration." + name, name,
                        new String[] { mapperMeta.getPackageName() }, nestedParameters, emptyList(), metadata, false);
            }).collect(toList()));

            return new ArrayList<>(singletonList(new ParameterMeta(null, Object.class, ParameterMeta.Type.OBJECT, "configuration",
                    "configuration",
                    new String[] { mapperMeta.getPackageName() } /* for i18n we use the mapper one which owns the virtual comp */,
                    nestedParameters, emptyList(), singletonMap("extension::virtual", "true"), false)));
        });
    }

    private String getVersionOptionName(final ComponentFamilyMeta.BaseMeta<?> meta) {
        return "$" + meta.getName() + "Version";
    }

    private Map<String, String> prefixWith(final String prefix, final Map<String, String> config) {
        return config.entrySet().stream().collect(toMap(e -> prefix + e.getKey(), Map.Entry::getValue));
    }

    private Stream<ParameterMeta> prefixWith(final String prefix, final List<ParameterMeta> parameterMetas) {
        return parameterMetas == null ? Stream.empty()
                : parameterMetas.stream()
                        .map(it -> new ParameterMeta(it.getSource(), it.getJavaType(), it.getType(), prefix + '.' + it.getPath(),
                                it.getName(), it.getI18nPackages(),
                                prefixWith(prefix, it.getNestedParameters()).collect(toList()), it.getProposals(),
                                it.getMetadata(), it.isLogMissingResourceBundle()));
    }

    @RequiredArgsConstructor
    private static class MapperWithChains {

        private final ComponentFamilyMeta.PartitionMapperMeta mapperMeta;

        private final VirtualChain[] chains;
    }

    @RequiredArgsConstructor
    private static class MapperWithChain {

        private final ComponentFamilyMeta.PartitionMapperMeta mapperMeta;

        private final VirtualChain chain;
    }
}
