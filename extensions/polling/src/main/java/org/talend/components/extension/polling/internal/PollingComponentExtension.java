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
package org.talend.components.extension.polling.internal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.extension.polling.api.Pollable;
import org.talend.components.extension.polling.api.PollableDuplicateDataset;
import org.talend.components.extension.polling.internal.impl.PollingConfiguration;
import org.talend.components.extension.polling.internal.impl.PollingMapper;
import org.talend.components.extension.register.api.CustomComponentExtension;
import org.talend.sdk.component.api.component.MigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.injector.Injector;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.design.extension.DesignModel;
import org.talend.sdk.component.design.extension.RepositoryModel;
import org.talend.sdk.component.design.extension.repository.Config;
import org.talend.sdk.component.design.extension.repository.ConfigKey;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.ParameterMeta;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.reflect.parameterenricher.BaseParameterEnricher;
import org.talend.sdk.component.runtime.manager.util.IdGenerator;
import org.talend.sdk.component.runtime.manager.util.Lazy;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * todo: ComponentValidator to validate the PollingMapper (ie i18n, ...)
 * IMPORTANT: this is not handled by the framework so validations must be coded in a
 * ComponentValidator extension!
 */
@Slf4j
public class PollingComponentExtension implements CustomComponentExtension {

    public final static String DUPLICATE_DATASET_KEY = PollingComponentExtension.class.getName() + ".duplicatedataset";

    public static final String VERSION_SUFFIX = "Version";

    public static final String ROOT_CONFIGURATION_KEY = "configuration";

    public static final String POLLING_CONFIGURATION_KEY = "pollingConfiguration";

    public static final String POLLING_CONFIGURATION_VERSION_KEY = "$" + POLLING_CONFIGURATION_KEY + VERSION_SUFFIX;

    public static final String POLLING_EXTENSION_CONFIGURATION_KEY = "extension::pollable";

    public static final String DEFAULT_POLLABLE_NAME_SUFFIX = "Pollable";

    public static final String METHOD_WITH_POLLING_CONFIGURATION_OPTION = "methodWithPollingConfigurationOption";

    @Override
    public Optional<Stream<Runnable>> onCreate(final Container container) {

        // Load the configuration
        final LocalConfiguration localConfiguration = LocalConfiguration.class
                .cast(container.get(ComponentManager.AllServices.class).getServices().get(LocalConfiguration.class));

        // Do we need to duplicate dataset ?
        final boolean duplicateDatasetOption = Boolean
                .valueOf(Optional.ofNullable(localConfiguration.get(DUPLICATE_DATASET_KEY)).orElse("true"));

        // Duplicate dataset, needed while pipeline designer need only one connector by dataset
        final List<String> duplicatedDataSets = duplicatePollableDataset(container, duplicateDatasetOption);
        if (duplicatedDataSets.isEmpty() && duplicateDatasetOption) {
            return Optional.empty();
        }

        // Duplicate mappers if needed
        return getContainerComponentFamilies(container).map(families -> families.flatMap(family -> {
            processFamily(container, family, duplicatedDataSets, duplicateDatasetOption);
            return Stream.empty(); // no clean up task here
        }));
    }

    private List<String> duplicatePollableDataset(final Container container, boolean duplicateDatasetOption) {
        log.debug("{} option : {}", DUPLICATE_DATASET_KEY, duplicateDatasetOption);
        if (!duplicateDatasetOption) {
            return emptyList();
        }

        // Class.forName(container.get(RepositoryModel.class).getContainerComponentFamilies().get(0).getConfigs().get().get(0).getChildConfigs().get(0).getMeta().getJavaType().getTypeName()).getAnnotations()
        final RepositoryModel repositoryModel = container.get(RepositoryModel.class);
        if (repositoryModel == null) {
            log.info("{} not loaded, can't duplicate {} dataset.", RepositoryModel.class.getName(),
                    PollableDuplicateDataset.class.getName());
            return emptyList();
        }

        return repositoryModel.getFamilies().stream()
                .flatMap(family -> recursiveDuplicatePollableDataset(family.getConfigs().get(), family.getMeta()).stream())
                .collect(toList());
    }

    private Collection<String> recursiveDuplicatePollableDataset(final List<Config> configs,
            final ComponentFamilyMeta familyMeta) {
        return new ArrayList<>(configs).stream().flatMap(config -> {
            if ("dataset".equals(config.getKey().getConfigType())) {
                final Class<?> datasetClass = Class.class.cast(config.getMeta().getJavaType());
                final PollableDuplicateDataset annotation = datasetClass.getAnnotation(PollableDuplicateDataset.class);

                if (annotation != null) {
                    final Config duplicate = new Config();
                    duplicate.setIcon(annotation.icon().isEmpty() ? config.getIcon() : annotation.icon());
                    final ConfigKey keyForDuplicate = getKeyForDuplicate(config.getKey());
                    duplicate.setKey(keyForDuplicate);
                    duplicate.setMeta(getMetaForDuplicate(config.getMeta()));
                    duplicate.setId(IdGenerator.get(familyMeta.getPlugin(), keyForDuplicate.getFamily(),
                            keyForDuplicate.getConfigType(), keyForDuplicate.getConfigName()));
                    duplicate.setVersion(config.getVersion());
                    duplicate.setMigrationHandler(config.getMigrationHandler());

                    configs.add(duplicate);

                    return Stream.of(config.getKey().getConfigName());
                }

            }
            return recursiveDuplicatePollableDataset(config.getChildConfigs(), familyMeta).stream();
        }).collect(toList());
    }

    private ConfigKey getKeyForDuplicate(final ConfigKey origin) {
        return new ConfigKey(origin.getFamily(), origin.getConfigName() + PollableDuplicateDataset.DUPLICATE_SUFFIX,
                origin.getConfigType());
    }

    private ParameterMeta getMetaForDuplicate(final ParameterMeta origin) {
        HashMap<String, String> meta = new HashMap<>(origin.getMetadata());
        meta.put("tcomp::configurationtype::name",
                origin.getMetadata().get("tcomp::configurationtype::name") + PollableDuplicateDataset.DUPLICATE_SUFFIX);
        return new ParameterMeta(origin.getSource(), origin.getJavaType(), origin.getType(), origin.getPath(), origin.getName(),
                origin.getI18nPackages(), origin.getNestedParameters(), origin.getProposals(), meta,
                origin.isLogMissingResourceBundle());
    }

    // Duplicate mappers
    private void processFamily(final Container container, final ComponentFamilyMeta family, final List<String> duplicateDatasets,
            boolean duplicateDatasetOption) {

        if (duplicateDatasets.isEmpty() && duplicateDatasetOption) {
            return;
        }

        // Retrieve pollable mappers
        final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers = family.getPartitionMappers();
        final List<PollableModel> pollables = findPollables(mappers);
        if (pollables.isEmpty()) {
            return;
        }

        final ComponentManager.AllServices allServices = container.get(ComponentManager.AllServices.class);
        final Map<Class<?>, Object> services = allServices.getServices();
        // Cache needed services
        final NeededServices neededServices = new NeededServices(services);

        // Duplicate @Pollable mappers to the real Polling mappers
        final List<ComponentFamilyMeta.PartitionMapperMeta> newMappersMeta = pollables.stream()
                .map(it -> toPartitionMapperMeta(it, allServices, neededServices, duplicateDatasets, duplicateDatasetOption))
                .collect(toList());

        // Now add all new partition mapper
        final Map<String, ComponentFamilyMeta.PartitionMapperMeta> newMappersMetaMap = newMappersMeta.stream()
                .collect(toMap(ComponentFamilyMeta.BaseMeta::getName, identity()));
        newMappersMetaMap.keySet().stream().forEach(p -> log.info("Created {} pollable metadata.", p));
        mappers.putAll(newMappersMetaMap);
    }

    // Duplicate a mapper to pollable one
    private ComponentFamilyMeta.PartitionMapperMeta toPartitionMapperMeta(final PollableModel model,
            final ComponentManager.AllServices allServices, final NeededServices neededServices,
            final List<String> duplicateDatasets, boolean duplicateDatasetOption) {

        // If Dataset has been duplicated, must be reported in mapper meta
        final ComponentFamilyMeta.PartitionMapperMeta srcMapperMeta = (duplicateDatasetOption)
                ? duplicateDatasetInMeta(model.srcMapperMeta, duplicateDatasets)
                : model.srcMapperMeta;

        // Compute the version of the new mapper : srcMapper.version + pollingConfiguration.version
        final Version pollingConfigurationAnnotationVersion = PollingConfiguration.class.getAnnotation(Version.class);
        final int version = srcMapperMeta.getVersion() + pollingConfigurationAnnotationVersion.value();

        final AtomicReference<Supplier<List<ParameterMeta>>> pollingParametersRef = new AtomicReference<>();

        final String pollableName = model.pollableAnnotation.name().isEmpty()
                ? srcMapperMeta.getName() + DEFAULT_POLLABLE_NAME_SUFFIX
                : model.pollableAnnotation.name();

        log.info("Duplicate mapper {} to its pollable version {}.", srcMapperMeta.getName(), pollableName);
        final ComponentFamilyMeta.PartitionMapperMeta partitionMapperMeta = new ComponentFamilyMeta.PartitionMapperMeta(
                srcMapperMeta.getParent(), // Keep the same parent
                pollableName, // The pollable name
                model.pollableAnnotation.icon().isEmpty() ? srcMapperMeta.getIcon() : model.pollableAnnotation.icon(), // set icon
                version, // Version of the pollable
                srcMapperMeta.getType(), // Not important, only used for validation
                createConfigurationSupplier(srcMapperMeta, neededServices, pollingConfigurationAnnotationVersion,
                        pollingParametersRef), // THe configuration
                createInstantiator(srcMapperMeta.getName(), srcMapperMeta, pollingParametersRef, neededServices), // The
                                                                                                                  // instanciator
                createMigrationHandlerSupplier(version, srcMapperMeta, allServices, neededServices, pollingParametersRef), // The
                                                                                                                           // migration
                                                                                                                           // handler
                false, // not needed
                true // It is a streaming connector now
        ) {
            // since constructor is protected
        };
        partitionMapperMeta.set(DesignModel.class, srcMapperMeta.get(DesignModel.class));

        return partitionMapperMeta;
    }

    private ComponentFamilyMeta.PartitionMapperMeta duplicateDatasetInMeta(ComponentFamilyMeta.PartitionMapperMeta srcMapperMeta,
            final List<String> duplicateDatasetOption) {
        if (duplicateDatasetOption.isEmpty()) {
            return srcMapperMeta;
        }

        // Rename dataset with @PollableDuplicateDataset#DUPLICATE_SUFFIX
        final Supplier<List<ParameterMeta>> lazyParameterMeta = Lazy
                .lazy(() -> copyParameters(srcMapperMeta.getParameterMetas().get(), meta -> {
                    final String type = meta.get("tcomp::configurationtype::type");
                    if ("dataset".equals(type)) {
                        final String name = meta.get("tcomp::configurationtype::name");
                        final Map<String, String> copy = new HashMap<>(meta);
                        copy.put("tcomp::configurationtype::name", name + PollableDuplicateDataset.DUPLICATE_SUFFIX);
                        return copy;
                    }
                    return meta;
                }));

        final ComponentFamilyMeta.PartitionMapperMeta newPartitionMapperMeta = new ComponentFamilyMeta.PartitionMapperMeta(
                srcMapperMeta.getParent(), srcMapperMeta.getName(), srcMapperMeta.getIcon(), srcMapperMeta.getVersion(),
                srcMapperMeta.getType(), lazyParameterMeta, srcMapperMeta.getInstantiator(), srcMapperMeta.getMigrationHandler(),
                srcMapperMeta.isValidated(), srcMapperMeta.isInfinite()) {
            // since constructor is protected
        };
        newPartitionMapperMeta.set(DesignModel.class, srcMapperMeta.get(DesignModel.class));

        return newPartitionMapperMeta;
    }

    private List<ParameterMeta> copyParameters(final List<ParameterMeta> rawParams,
            final Function<Map<String, String>, Map<String, String>> metaMapper) {
        return rawParams.stream().map(m -> copyParameterMeta(m, metaMapper)).collect(toList());
    }

    private ParameterMeta copyParameterMeta(final ParameterMeta m,
            final Function<Map<String, String>, Map<String, String>> metaMapper) {
        return new ParameterMeta(m.getSource(), m.getJavaType(), m.getType(), m.getPath(), m.getName(), m.getI18nPackages(),
                m.getNestedParameters() != null ? copyParameters(m.getNestedParameters(), metaMapper) : null, m.getProposals(),
                metaMapper.apply(m.getMetadata()), m.isLogMissingResourceBundle());
    }

    private Supplier<MigrationHandler> createMigrationHandlerSupplier(final int currentVersion,
            final ComponentFamilyMeta.PartitionMapperMeta mapperMeta, final ComponentManager.AllServices services,
            final NeededServices neededServices, final AtomicReference<Supplier<List<ParameterMeta>>> pollingParameters) {
        // incomingVersion: version of the PollingConfiguration used when incomingData has been serialized
        return Lazy.lazy(() -> (incomingVersion, incomingData) -> {

            if (incomingData == null || incomingVersion == currentVersion) {
                return incomingData;
            }

            // here we must split the incoming data per component, migrate it and re-prefix it
            final Map<String, Map<String, String>> confs = splitConfigurationPerSubComponent(incomingData);

            // Migration of the sub-configuration of the mapper
            final Map<String, String> migratedMapperConfiguration = mapperMeta.getMigrationHandler().get().migrate(
                    findStoreSubComponentVersion(incomingData, mapperMeta),
                    confs.getOrDefault(mapperMeta.getName(), new HashMap<>()));

            // Migration of the polling configuration
            final Supplier<List<ParameterMeta>> pollingConfigurationParameters = getPollingConfigurationParameters(
                    pollingParameters, neededServices, mapperMeta.getType().getPackage());
            final MigrationHandler pollingConfigurationMigrationHandler = ComponentManager.instance().getMigrationHandlerFactory()
                    .findMigrationHandler(pollingConfigurationParameters, PollingConfiguration.class, services);
            final Map<String, String> migratedPollingConfiguration = pollingConfigurationMigrationHandler.migrate(
                    Integer.parseInt(incomingData.getOrDefault("configuration." + POLLING_CONFIGURATION_VERSION_KEY, "0")),
                    confs.getOrDefault(POLLING_CONFIGURATION_KEY, new HashMap<>()));

            final Map<String, String> resultingMigration = new HashMap<>();
            resultingMigration.putAll(prefixWith("configuration." + mapperMeta.getName(), migratedMapperConfiguration));
            resultingMigration.putAll(prefixWith("configuration." + POLLING_CONFIGURATION_KEY, migratedPollingConfiguration));

            // passthrough internal configs ($ in keys)
            resultingMigration.putAll(incomingData.entrySet().stream()
                    .filter(it -> !resultingMigration.containsKey(it.getKey()) && it.getKey().contains("$"))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));

            return resultingMigration;
        });
    }

    private Supplier<List<ParameterMeta>> getPollingConfigurationParameters(
            final AtomicReference<Supplier<List<ParameterMeta>>> pollingParameters, final NeededServices services,
            final Package i18nPackage) {
        Supplier<List<ParameterMeta>> params = pollingParameters.get();
        if (params == null) {
            final List<ParameterMeta> metas = getPoolingRawMetas(services, i18nPackage);
            params = () -> metas;
            pollingParameters.compareAndSet(null, params);
        }
        return params;
    }

    private int findStoreSubComponentVersion(final Map<String, String> incomingData, final ComponentFamilyMeta.BaseMeta<?> meta) {
        try {
            return Integer.parseInt(incomingData.getOrDefault(ROOT_CONFIGURATION_KEY + "." + getVersionOptionName(meta), "0"));
        } catch (final NumberFormatException nfe) {
            log.warn("No version found for " + meta.getName());
            return 0;
        }
    }

    private List<PollableModel> findPollables(final Map<String, ComponentFamilyMeta.PartitionMapperMeta> mappers) {
        return mappers.values().stream().map(it -> new PollableModel(it, it.getType().getAnnotation(Pollable.class)))
                .filter(p -> p.pollableAnnotation != null).collect(toList());
    }

    private Function<Map<String, String>, Mapper> createInstantiator(final String name,
            final ComponentFamilyMeta.PartitionMapperMeta initialMapperMeta,
            final AtomicReference<Supplier<List<ParameterMeta>>> pollingParameters, NeededServices neededServices) {
        return configuration -> {
            // split configuration
            final Map<String, Map<String, String>> confs = splitConfigurationPerSubComponent(configuration);

            // Create original mapper instantiator
            final Mapper batchMapper = initialMapperMeta.getInstantiator().apply(confs.get(name));

            // Retrieve polling configuration factory
            Function<Map<String, String>, Object[]> pollingConfigurationFactory = null;
            try {

                final Supplier<List<ParameterMeta>> pollingConfigurationParameters = getPollingConfigurationParameters(
                        pollingParameters, neededServices, initialMapperMeta.getType().getPackage());

                pollingConfigurationFactory = neededServices.reflectionService
                        .parameterFactory(
                                PollingComponentExtension.class.getMethod(METHOD_WITH_POLLING_CONFIGURATION_OPTION,
                                        PollingConfiguration.class),
                                neededServices.services, pollingConfigurationParameters.get());

            } catch (NoSuchMethodException e) {
                log.error("Can't retrieve the method with the polling configuration.", e);
            }

            // Instance the polling configuration object from main configuration map
            final PollingConfiguration pollingConfiguration = PollingConfiguration.class
                    .cast(pollingConfigurationFactory.apply(confs.get(POLLING_CONFIGURATION_KEY))[0]);

            return newMapper(batchMapper.plugin(), new PollingMapper(pollingConfiguration, batchMapper));
        };
    }

    /**
     * Explode configuration in a map with at least two key : name of main connector & POLLING_CONFIGURATION_KEY
     *
     * @param configuration
     * @return
     */
    private Map<String, Map<String, String>> splitConfigurationPerSubComponent(final Map<String, String> configuration) {
        return configuration.entrySet().stream()
                .filter(it -> it.getKey().startsWith(ROOT_CONFIGURATION_KEY + ".") && findRealKeyIndex(it.getKey()) > 0)
                .collect(groupingBy(
                        e -> e.getKey().substring((ROOT_CONFIGURATION_KEY + ".").length(), findRealKeyIndex(e.getKey())),
                        toMap(e -> e.getKey().substring(findRealKeyIndex(e.getKey()) + 1), Map.Entry::getValue)));
    }

    private int findRealKeyIndex(final String key) {
        return key.indexOf('.', (ROOT_CONFIGURATION_KEY + ".").length() + 1);
    }

    /**
     * Not used by the instanciator since we overload it in this extension, but will be used by the component server for form.
     */
    private Supplier<List<ParameterMeta>> createConfigurationSupplier(final ComponentFamilyMeta.PartitionMapperMeta mapperMeta,
            NeededServices neededServices, Version pollingConfigurationVersion,
            final AtomicReference<Supplier<List<ParameterMeta>>> pollingParametersRef) {
        return Lazy.lazy(() -> {

            final List<ParameterMeta> pollingRawMetas = getPollingConfigurationParameters(pollingParametersRef, neededServices,
                    mapperMeta.getType().getPackage()).get();

            final Map<String, String> rootPollingMetadata = new HashMap<>();
            rootPollingMetadata.put("ui::gridlayout::Main::value", "internal_polling_configuration");
            rootPollingMetadata.put("ui::gridlayout::Advanced::value", ""); // No advanced option for polling currently

            final ParameterMeta rootPollingMeta = new ParameterMeta(null, Object.class, ParameterMeta.Type.OBJECT,
                    POLLING_CONFIGURATION_KEY, POLLING_CONFIGURATION_KEY, new String[] { mapperMeta.getPackageName() },
                    prefixWith(POLLING_CONFIGURATION_KEY, pollingRawMetas).collect(toList()), emptyList(), rootPollingMetadata,
                    false);

            final List<ParameterMeta> delegateMetas = mapperMeta.getParameterMetas().get().stream().collect(toList());
            final Map<String, String> rootDelegateMetadata = new HashMap<>();
            List<String> delegateNames = getNames(delegateMetas);
            rootDelegateMetadata.put("ui::gridlayout::Main::value", String.join("|", delegateNames));
            rootDelegateMetadata.put("ui::gridlayout::Advanced::value", String.join("|", delegateNames));

            final ParameterMeta rootDelegateMeta = new ParameterMeta(null, Object.class, ParameterMeta.Type.OBJECT,
                    mapperMeta.getName(), mapperMeta.getName(), new String[] { mapperMeta.getPackageName() },
                    prefixWith(mapperMeta.getName(), delegateMetas).collect(toList()), emptyList(), rootDelegateMetadata, false);

            final List<ParameterMeta> parameters = Stream
                    .of(prefixWith(ROOT_CONFIGURATION_KEY, rootDelegateMeta), prefixWith(ROOT_CONFIGURATION_KEY, rootPollingMeta))
                    .flatMap(identity()).collect(toList());

            // now we add for each component a hidden configuration hosting (thanks to the default) the actual version
            // of the component to be able to impl the migration
            // The hidden fields with version will be serialized within connector configuration
            {
                // 1st bloc to store mapper version
                final String name = getVersionOptionName(mapperMeta);
                final Map<String, String> metadata = new HashMap<>();
                // Anotated as synthetic since created on the flow
                metadata.put(POLLING_EXTENSION_CONFIGURATION_KEY + "::synthetic", "true");
                metadata.put("tcomp::ui::defaultvalue::value", Integer.toString(mapperMeta.getVersion()));
                // hide the 'name' property which contains the version of the initial mapper
                metadata.put("tcomp::condition::if::target", "missing");
                metadata.put("tcomp::condition::if::value", "true");
                metadata.put("tcomp::condition::if::negate", "false");
                metadata.put("tcomp::condition::if::evaluationStrategy", "DEFAULT");

                parameters.add(new ParameterMeta(null, int.class, ParameterMeta.Type.NUMBER, ROOT_CONFIGURATION_KEY + "." + name,
                        name, new String[] { mapperMeta.getPackageName() }, emptyList(), emptyList(), metadata, false));
            }

            {
                // 2st bloc to store PollingConfiguration version

                // main param
                final String name = POLLING_CONFIGURATION_VERSION_KEY;
                final Map<String, String> metadata = new HashMap<>();
                // Generated on the flow
                metadata.put(POLLING_EXTENSION_CONFIGURATION_KEY + "::synthetic", "true");
                metadata.put("tcomp::ui::defaultvalue::value", Integer.toString(pollingConfigurationVersion.value()));
                // hide the 'name' property which contains the version of the initial mapper
                metadata.put("tcomp::condition::if::target", "missing");
                metadata.put("tcomp::condition::if::value", "true");
                metadata.put("tcomp::condition::if::negate", "false");
                metadata.put("tcomp::condition::if::evaluationStrategy", "DEFAULT");

                parameters.add(new ParameterMeta(null, int.class, ParameterMeta.Type.NUMBER, ROOT_CONFIGURATION_KEY + "." + name,
                        name, new String[] { mapperMeta.getPackageName() }, emptyList(), emptyList(), metadata, false));
            }

            List<String> parametersName = parameters.stream().filter(e -> '$' != e.getName().charAt(0)).map(e -> e.getName())
                    .collect(toList());

            final Map<String, String> rootMetadata = new HashMap<>();
            rootMetadata.put("ui::gridlayout::Main::value", String.join("|", parametersName));
            rootMetadata.put("ui::gridlayout::Advanced::value", String.join("|", parametersName));

            rootMetadata.put(POLLING_EXTENSION_CONFIGURATION_KEY, String.valueOf(Boolean.TRUE));

            // Add just the root level of the configuration.
            final List<ParameterMeta> rootParameterMetas = singletonList(new ParameterMeta(null, Object.class,
                    ParameterMeta.Type.OBJECT, ROOT_CONFIGURATION_KEY, ROOT_CONFIGURATION_KEY,
                    new String[] { mapperMeta.getPackageName(), PollingConfiguration.class.getPackage()
                            .getName() } /* for i18n we use the mapper one which owns the virtual comp */,
                    parameters, emptyList(), rootMetadata, false));

            return rootParameterMetas;
        });
    }

    private List<String> getNames(List<ParameterMeta> parameters) {
        return parameters.stream().filter(e -> '$' != e.getName().charAt(0)).map(e -> e.getName()).collect(toList());
    }

    private String getVersionOptionName(final ComponentFamilyMeta.BaseMeta<?> meta) {
        return "$" + meta.getName() + VERSION_SUFFIX;
    }

    private Map<String, String> prefixWith(final String prefix, final Map<String, String> config) {
        return config.entrySet().stream().collect(toMap(e -> prefix + "." + e.getKey(), Map.Entry::getValue));
    }

    private Stream<ParameterMeta> prefixWith(final String prefix, final ParameterMeta parameterMetas) {
        return prefixWith(prefix, Collections.singletonList(parameterMetas));
    }

    private Stream<ParameterMeta> prefixWith(final String prefix, final List<ParameterMeta> parameterMetas) {
        return parameterMetas == null ? Stream.empty()
                : parameterMetas.stream()
                        .map(it -> new ParameterMeta(it.getSource(), it.getJavaType(), it.getType(), prefix + '.' + it.getPath(),
                                it.getName(), it.getI18nPackages(),
                                prefixWith(prefix, it.getNestedParameters()).collect(toList()), it.getProposals(),
                                it.getMetadata(), it.isLogMissingResourceBundle()));
    }

    /**
     * Return metadata of the @option of the method methodWithPollingConfigurationOption.
     * It means the metadata of the specific configuration of the polling.
     *
     * @param
     * @return
     */
    private List<ParameterMeta> getPoolingRawMetas(final NeededServices neededServices, final Package i18nPackage) {
        try {
            return neededServices.parameterModelService.buildParameterMetas(
                    getClass().getMethod(METHOD_WITH_POLLING_CONFIGURATION_OPTION, PollingConfiguration.class),
                    i18nPackage == null ? "" : i18nPackage.getName(),
                    new BaseParameterEnricher.Context(neededServices.localConfig));
        } catch (final Exception e) {
            // Means that methodWithPollingConfigurationOption has not been found
            throw new IllegalStateException(e);
        }
    }

    // This method exists only to have the @Option with the PollingConfiguration class
    public void methodWithPollingConfigurationOption(
            @Option("internal_polling_configuration") final PollingConfiguration pollingConfiguration) {
        // no-op
    }

    // Util class that contain the initial Meta and the Pollable annotation
    @RequiredArgsConstructor
    private static class PollableModel {

        private final ComponentFamilyMeta.PartitionMapperMeta srcMapperMeta;

        private final Pollable pollableAnnotation;

    }

    // Cache all needed services
    private static class NeededServices {

        private final Map<Class<?>, Object> services;

        private final LocalConfiguration localConfig;

        private final ParameterModelService parameterModelService;

        private final ReflectionService reflectionService;

        private NeededServices(final Map<Class<?>, Object> services) {
            this.services = services;
            this.localConfig = LocalConfiguration.class.cast(services.get(LocalConfiguration.class));

            final Injector injector = Injector.class.cast(services.get(Injector.class)); // hack to retrieve the parameter service
            // which is not exposed
            reflectionService = get(injector, "reflectionService", ReflectionService.class);

            parameterModelService = get(reflectionService, "parameterModelService", ParameterModelService.class);
        }

        private <T> T get(final Object from, final String name, final Class<T> type) {
            try {
                final Field reflectionService = from.getClass().getDeclaredField(name);
                if (!reflectionService.isAccessible()) {
                    reflectionService.setAccessible(true);
                }
                return type.cast(reflectionService.get(from));
            } catch (final Exception e) {
                throw new IllegalStateException("Incompatible component runtime manager", e);
            }
        }

    }

}
