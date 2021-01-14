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
package org.talend.components.common.stream.format.line.fixed;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.fixed.FixedConfiguration;
import org.talend.components.common.stream.input.fixed.FixedReaderSupplier;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.common.stream.api")
class FixedFormatTest {

    @Service
    private RecordIORepository repo;

    @Test
    void format() {
        final RecordReaderSupplier reader = repo.findReader(FixedConfiguration.class);
        Assertions.assertNotNull(reader);
        Assertions.assertTrue(FixedReaderSupplier.class.isInstance(reader));
    }
}