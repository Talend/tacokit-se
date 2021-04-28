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
package org.talend.components.adlsgen2.runtime.output;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.adlsgen2")
public class BlobWriterTest extends AdlsGen2TestBase {

    static BlobWriter blobWriter;

    @BeforeEach
    public void setup() throws Exception {
        super.setUp();
        blobWriter = new ParquetBlobWriter(outputConfiguration, recordBuilderFactory, jsonBuilderFactory, service,
                tokenProviderService);
    }

    @Test
    public void testCompleteOnZeroInputRecords() {
        /*
         * There was a NPE issue.
         * No batch was created, since the {@link BlobWriter#newBatch()} was not called.
         */
        Assertions.assertDoesNotThrow(blobWriter::complete);
    }
}
