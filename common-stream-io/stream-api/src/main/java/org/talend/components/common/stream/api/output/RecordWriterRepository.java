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
package org.talend.components.common.stream.api.output;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.talend.components.common.stream.format.ContentFormat;

public class RecordWriterRepository {

    private ConcurrentMap<Class<? extends ContentFormat>, RecordWriterSupplier<?>> suppliers = new ConcurrentHashMap<>();

    private RecordWriterRepository() {
    }

    /** Holder */
    private static class RepositoryHolder {

        /** Single instance (lzay initilized) */
        private final static RecordWriterRepository instance = new RecordWriterRepository();
    }

    /** Point d'accès pour l'instance unique du singleton */
    public static RecordWriterRepository getInstance() {
        return RecordWriterRepository.RepositoryHolder.instance;
    }

    public <T extends ContentFormat, U> void put(Class<T> configClazz, RecordWriterSupplier<U> factory) {
        this.suppliers.put(configClazz, factory);
    }

    public <T extends ContentFormat> RecordWriterSupplier<?> get(Class<T> configClazz) {
        return this.suppliers.get(configClazz);
    }

}
