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
 *
 */
package org.talend.components.adlsgen2.common.format.csv;

import java.io.InputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.csv.CsvIterator.Builder;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;

import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@WithComponents("org.talend.components.adlsgen2")
public class CsvConverterTest extends AdlsGen2TestBase {

    private CsvConfiguration csvConfiguration;

    private CsvConverter converter;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void csvWithPipeAsDelimiterCase() throws Exception {
        InputStream sample = getClass().getResource("/common/format/csv/pipe-separated.csv").openStream();
        csvConfiguration = new CsvConfiguration();
        csvConfiguration.setFieldDelimiter(CsvFieldDelimiter.OTHER);
        csvConfiguration.setCustomFieldDelimiter("|");
        CsvIterator it = Builder.of(recordBuilderFactory).withConfiguration(csvConfiguration).parse(sample);
        int counted = 0;
        while (it.hasNext()) {
            Record record = it.next();
            assertNotNull(record);
            counted++;
        }
        assertEquals(6, counted);
    }

}
