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
package org.talend.components.common.stream.api.input;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.talend.components.common.stream.format.ContentFormat;

public class RecordReaderRepository {

    private ConcurrentMap<Class<? extends ContentFormat>, RecordReaderSupplier> suppliers = new ConcurrentHashMap<>();

    private RecordReaderRepository() {
    }

    /** Holder */
    private static class RepositoryHolder {

        /** Single instance (lzay initilized) */
        private final static RecordReaderRepository instance = new RecordReaderRepository();
    }

    /** Point d'accès pour l'instance unique du singleton */
    public static RecordReaderRepository getInstance() {
        return RepositoryHolder.instance;
    }

    public <T extends ContentFormat> void put(Class<T> clazz, RecordReaderSupplier factory) {
        this.suppliers.put(clazz, factory);
    }

    public <T extends ContentFormat> RecordReaderSupplier get(Class<T> clazz) {
        return (RecordReaderSupplier) this.suppliers.get(clazz);
    }
}
