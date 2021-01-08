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
package org.talend.components.common.stream.api;

import java.util.Locale;
import java.util.ResourceBundle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.api.reader.FakeReaderSupplier;
import org.talend.components.common.stream.api.writer.FakeWriterSupplier;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.common.stream.api")
class RecordIORepositoryTest {

    @Service
    private RecordIORepository repo;

    @Test
    public void testRepo() {

        final RecordReaderSupplier reader = this.repo.findReader(FakeConfig.class);
        Assertions.assertNotNull(reader, "reader not found");
        Assertions.assertTrue(FakeReaderSupplier.class.isInstance(reader),
                () -> "wrong reader class " + reader.getClass().getName());

        final RecordWriterSupplier writer = this.repo.findWriter(FakeConfig.class);
        Assertions.assertNotNull(writer, "writer not found");
        Assertions.assertTrue(FakeWriterSupplier.class.isInstance(writer),
                () -> "wrong reader class " + writer.getClass().getName());
    }

    @Test
    public void te2() {
        final String baseName = Messages.class.getPackage().getName() + ".Messages";
        final ResourceBundle bundle = ResourceBundle.getBundle(baseName, Locale.getDefault(),
                Thread.currentThread().getContextClassLoader());
        Assertions.assertNotNull(bundle, "bundle not found");

    }

}