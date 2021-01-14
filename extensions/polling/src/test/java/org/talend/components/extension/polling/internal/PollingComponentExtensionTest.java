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

import org.junit.jupiter.api.Test;
import org.talend.components.extension.polling.api.Pollable;
import org.talend.components.extension.polling.api.PollableDuplicateDataset;
import org.talend.components.extension.polling.internal.impl.PollingConfiguration;
import org.talend.components.extension.polling.internal.impl.PollingMapper;
import org.talend.components.extension.polling.mapperA.BatchConfig;
import org.talend.components.extension.polling.mapperA.BatchSource;
import org.talend.components.extension.register.api.Contextual;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.container.ContainerManager;
import org.talend.sdk.component.design.extension.RepositoryModel;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.ContainerComponentRegistry;
import org.talend.sdk.component.runtime.manager.ParameterMeta;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.extension.polling.mapperA")
public class PollingComponentExtensionTest {

    public final static int TEST_POLLING_DELAY = 100;

    public final static int TEST_NB_EXPECTED_ROWS = 3;

    @Injected
    private BaseComponentsHandler handler;

    private Supplier<Container> container = () -> {
        final ContainerManager containerManager = ComponentManager.instance().getContainer();
        return containerManager.find(handler.getTestPlugins().iterator().next())
                .orElseThrow(() -> new IllegalStateException("At least one plugin, 'test/classes', must be loaded"));
    };

    @Test
    void testSerialization() throws IOException, ClassNotFoundException {
        final Optional<Mapper> mapper = handler.asManager().findMapper("mytest", "MyPollable", 1, emptyMap());
        assertTrue(mapper.isPresent());
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final ObjectOutputStream objectOutputStream = new ObjectOutputStream(out)) {
            objectOutputStream.writeObject(mapper.get());
        }
        try (final ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()))) {
            final Object o = objectInputStream.readObject();
            assertTrue(Proxy.isProxyClass(o.getClass()));
            final InvocationHandler invocationHandler = Proxy.getInvocationHandler(o);
            assertTrue(Contextual.class.isInstance(invocationHandler));
            final Contextual contextual = Contextual.class.cast(invocationHandler);
            assertEquals(mapper.get().plugin(), contextual.getPlugin());
            assertTrue(PollingMapper.class.isInstance(contextual.getDelegate()));
        }
    }

    @Test
    void testPollableRuntime() {

        final BatchConfig mainConfig = new BatchConfig();
        mainConfig.setParam0(1234);
        mainConfig.setParam1("abcde");

        PollingConfiguration pollingConf = new PollingConfiguration();
        pollingConf.setDelay(TEST_POLLING_DELAY);

        final String mainConfigStr = configurationByExample().forInstance(mainConfig)
                .withPrefix("configuration.BatchSource.configuration").configured().toQueryString();

        final String pollingConfigStr = configurationByExample().forInstance(pollingConf).withPrefix(
                "configuration." + PollingComponentExtension.POLLING_CONFIGURATION_KEY + ".internal_polling_configuration")
                .configured().toQueryString();

        Job.components() //
                .component("emitter", "mytest://MyPollable?" + String.join("&", mainConfigStr, pollingConfigStr)) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(TEST_NB_EXPECTED_ROWS, records.size());

        for (int i = 1; i <= records.size(); i++) {
            assertEquals("Data/abcde" + i + "/" + i, records.get(i - 1).getString("valueStr"));
            assertEquals(1234 + i, records.get(i - 1).getInt("valueInt"));
        }
    }

    @Test
    void testMapperDuplication() {
        final ContainerComponentRegistry containerComponentRegistry = container.get().get(ContainerComponentRegistry.class);
        assertEquals(1, containerComponentRegistry.getComponents().size());

        final ComponentFamilyMeta components = containerComponentRegistry.getComponents().get("mytest");
        assertNotNull(components); // test family found
        assertEquals(3, components.getPartitionMappers().size());

        assertTrue(components.getPartitionMappers().containsKey(BatchSource.class.getAnnotation(Pollable.class).name()));

        Stream.of("BatchSource", "MyPollable", "UnusedSource").forEach(n -> {
            final ComponentFamilyMeta.PartitionMapperMeta partitionMapperMeta = components.getPartitionMappers().get(n);
            assertNotNull(partitionMapperMeta);
            assertEquals(n, partitionMapperMeta.getName());
        });
    }

    @Test
    void testDatasetDuplication() {
        final RepositoryModel repositoryModel = container.get().get(RepositoryModel.class);

        assertEquals(1, repositoryModel.getFamilies().size());
        assertEquals("mytest", repositoryModel.getFamilies().get(0).getMeta().getName());

        final List<String> mapperNames = repositoryModel.getFamilies().get(0).getMeta().getPartitionMappers().values().stream()
                .map(ComponentFamilyMeta.BaseMeta::getName).collect(Collectors.toList());
        assertEquals(3, mapperNames.size());
        assertTrue(mapperNames.remove("BatchSource"));
        assertTrue(mapperNames.remove("MyPollable")); // Duplicate of BatchSource
        assertTrue(mapperNames.remove("UnusedSource")); // UnusedSource Must not be duplicated
        assertEquals(0, mapperNames.size());

    }

    @Test
    void checkDataSetNameInMapperParameterModels() {
        final List<ParameterMeta> parameterMetas = container.get().get(ContainerComponentRegistry.class).getComponents()
                .get("mytest").getPartitionMappers().get("MyPollable").getParameterMetas().get();
        final List<String> datasetName = flatten(parameterMetas)
                .filter(it -> "dataset".equals(it.getMetadata().get("tcomp::configurationtype::type")))
                .map(it -> it.getMetadata().get("tcomp::configurationtype::name")).filter(Objects::nonNull).collect(toList());
        assertEquals(singletonList("batchDatasetNamePollable"), datasetName);
        assertNotNull(findParameterMeta(parameterMetas, "dataset", "batchDatasetNamePollable"));
    }

    @Test
    void checkDataSet() {
        final ParameterMeta parameterMetas = container.get().get(RepositoryModel.class).getFamilies().iterator().next()
                .getConfigs().get().stream().flatMap(it -> it.getChildConfigs().stream())
                .filter(it -> it.getKey().getConfigName().endsWith(PollableDuplicateDataset.DUPLICATE_SUFFIX)).iterator().next()
                .getMeta();
        assertNotNull(findParameterMeta(singletonList(parameterMetas), "dataset", "batchDatasetNamePollable"));
    }

    private Stream<ParameterMeta> flatten(final List<ParameterMeta> parameterMetas) {
        return parameterMetas.stream().flatMap(it -> it.getNestedParameters() == null ? Stream.of(it)
                : Stream.concat(Stream.of(it), flatten(it.getNestedParameters())));
    }

    // pd method
    public ParameterMeta findParameterMeta(List<ParameterMeta> in, String type, String name) {
        // If this matches, return success
        for (ParameterMeta pm : in) {
            if (pm.getType() == ParameterMeta.Type.OBJECT && type.equals(pm.getMetadata().get("tcomp::configurationtype::type"))
                    && name.equals(pm.getMetadata().get("tcomp::configurationtype::name"))) {
                return pm;
            }
            ParameterMeta recurse = findParameterMeta(pm.getNestedParameters(), type, name);
            if (recurse != null) {
                return recurse;
            }
        }
        return null;
    }

}