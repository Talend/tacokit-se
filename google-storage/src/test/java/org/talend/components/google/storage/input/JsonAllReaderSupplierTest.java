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
package org.talend.components.google.storage.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.google.storage.dataset.JsonAllConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class JsonAllReaderSupplierTest {

    @Test
    void getReader() {
        JsonAllConfiguration cfg = new JsonAllConfiguration();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        JsonAllReaderSupplier supplier = new JsonAllReaderSupplier();
        final RecordReader reader = supplier.getReader(factory, cfg);

        Assertions.assertNotNull(reader);
        Assertions.assertEquals(JsonAllRecordReader.class, reader.getClass());
    }
}