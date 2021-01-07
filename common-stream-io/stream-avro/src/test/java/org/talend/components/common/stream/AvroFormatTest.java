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
package org.talend.components.common.stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.components.common.stream.input.avro.AvroReaderSupplier;
import org.talend.components.common.stream.output.avro.AvroWriterSupplier;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.common.stream.api")
class AvroFormatTest {

    @Service
    private RecordIORepository repo;

    @Test
    void format() {
        final RecordReaderSupplier reader = repo.findReader(AvroConfiguration.class);
        Assertions.assertNotNull(reader);
        Assertions.assertTrue(AvroReaderSupplier.class.isInstance(reader));

        final RecordWriterSupplier writer = repo.findWriter(AvroConfiguration.class);
        Assertions.assertNotNull(writer);
        Assertions.assertTrue(AvroWriterSupplier.class.isInstance(writer));
    }
}