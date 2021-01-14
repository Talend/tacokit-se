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
package org.talend.components.extension.register.api;

import lombok.RequiredArgsConstructor;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class MapperResultWrapper implements Contextual.ResultWrapper, Serializable {

    private final String plugin;

    @Override
    public boolean handles(final Method method) {
        return Objects.equals(Mapper.class, method.getDeclaringClass())
                && ("create".equals(method.getName()) || "split".equals(method.getName()));
    }

    @Override
    public Object wrap(final Object value) {
        if (Input.class.isInstance(value)) { // create()
            return Contextual.proxy(Input.class, plugin, Input.class.cast(value), null);
        } else if (Collection.class.isInstance(value)) { // split()
            final Collection<Mapper> mappers = Collection.class.cast(value);
            return mappers.stream().map(Mapper.class::cast)
                    .map(mapper -> isAlreadyWrapped(mapper) ? mapper : Contextual.proxy(Mapper.class, plugin, mapper, this))
                    .collect(toList());
        } else if (value == null) {
            return null;
        }
        throw new UnsupportedOperationException("Unsupported value: " + value);
    }

    private boolean isAlreadyWrapped(final Mapper mapper) {
        return Proxy.class.isInstance(mapper) && Contextual.class.isInstance(Proxy.getInvocationHandler(mapper));
    }
}
