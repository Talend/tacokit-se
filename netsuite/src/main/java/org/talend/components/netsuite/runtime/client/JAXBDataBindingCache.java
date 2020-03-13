/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.netsuite.runtime.client;

import lombok.Getter;
import org.apache.cxf.jaxb.JAXBDataBinding;

import javax.xml.bind.JAXBException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JAXBDataBindingCache {

    @Getter
    private static JAXBDataBindingCache instance = new JAXBDataBindingCache();

    public final Map<List<Class<?>>, JAXBDataBinding> cache = new ConcurrentHashMap<>();

    private JAXBDataBindingCache() {
    }

    public JAXBDataBinding getBinding(Class<?>... classes) throws JAXBException {
        try {
            return cache.computeIfAbsent(Arrays.asList(classes), k -> {
                try {
                    return new JAXBDataBinding(classes);
                } catch (JAXBException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof JAXBException) {
                throw (JAXBException) e.getCause();
            }
            throw e;
        }
    }
}
