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
package org.talend.components.common.stream;

import java.util.Collections;

import javax.json.Json;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.common.stream.input.json.JsonReaderSupplier;
import org.talend.components.common.stream.output.json.JsonWriterSupplier;

class ExcelFormatTest {

    @Test
    void format() {
        RecordIORepository repo = new RecordIORepository(Json.createReaderFactory(Collections.emptyMap()));
        repo.init();

        final RecordReaderSupplier reader = repo.findReader(JsonConfiguration.class);
        Assertions.assertNotNull(reader);
        Assertions.assertTrue(JsonReaderSupplier.class.isInstance(reader));

        final RecordWriterSupplier writer = repo.findWriter(JsonConfiguration.class);
        Assertions.assertNotNull(writer);
        Assertions.assertTrue(JsonWriterSupplier.class.isInstance(writer));
    }
}