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

import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.component.Components;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.type.meta.ConfigurationType;
import org.talend.sdk.component.runtime.manager.reflect.parameterenricher.BaseParameterEnricher;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

/**
 * This BaseParameterEnricher is responsible for adding some metadata to the fields annotated with @DatastoreRef.
 * <p>
 * These metadata will be used by client applications to change the default display behavior of the annotated fields.
 * For example, in Pipeline Designer, the annotated field will be displayed with a closed drop down list listing the
 * connections of the right kind, available in the Pipeline Designer application.
 */
@Slf4j
public class DatastoreRefEnricher extends BaseParameterEnricher {

    private static final String META_PREFIX = "connectors::extensions::configurationtyperef::";

    private static final String DATASTORE_TYPE = "datastore";

    @Override
    public Map<String, String> onParameterAnnotation(final String parameterName, final Type parameterType,
            final Annotation annotation) {
        if (annotation instanceof DatastoreRef) {
            final DatastoreRef dsRef = (DatastoreRef) annotation;
            final HashMap<String, String> metas = new HashMap<>();

            metas.put(META_PREFIX + "id", dsRef.configurationId());
            metas.put(META_PREFIX + "type", DATASTORE_TYPE);

            final AtomicInteger index = new AtomicInteger(0);
            Stream.of(dsRef.filters()).forEach(f -> {
                final int i = index.getAndIncrement();
                metas.put(META_PREFIX + "filter[" + i + "].key", f.key());
                metas.put(META_PREFIX + "filter[" + i + "].value", f.value());
            });
            return metas;
        }
        return emptyMap();
    }
}
