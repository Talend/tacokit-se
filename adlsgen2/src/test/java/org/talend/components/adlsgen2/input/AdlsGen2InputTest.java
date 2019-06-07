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
package org.talend.components.adlsgen2.input;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvFieldDelimiter;
import org.talend.components.adlsgen2.common.format.csv.CsvRecordSeparator;
import org.talend.components.adlsgen2.common.format.json.JsonConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.http.internal.impl.AzureStorageCredentialsRemovalResponseLocator;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@org.talend.sdk.component.junit.http.junit5.HttpApi(useSsl = true, responseLocator = AzureStorageCredentialsRemovalResponseLocator.class)
@WithComponents("org.talend.components.adlsgen2")
class AdlsGen2InputTest extends AdlsGen2TestBase {

    @Test
    public void csvBasicCase() {
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "AdlsGen2://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(10000, records.size());
    }

    @Test
    public void csvWithUserSchema() {
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("IdCustomer;FirstName;lastname;address;enrolled;zip;state");
        csvConfig.setHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "AdlsGen2://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(500, records.size());
        Record record = records.get(0);
        // schema should comply file header
        assertNotNull(record.getString("IdCustomer"));
        assertNotNull(record.getString("FirstName"));
    }

    @Test
    public void csvWithInferHeaderFromRuntime() {
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("");
        csvConfig.setHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "AdlsGen2://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(500, records.size());
        Record record = records.get(0);
        // schema should comply file header
        assertNotNull(record.getString("id"));
        assertNotNull(record.getString("Firstname"));
    }

    @Test
    public void csvWithAutoGeneratedHeaders() {
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("");
        csvConfig.setHeader(false);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "AdlsGen2://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        // header is a record
        assertEquals(501, records.size());
        Record record = records.get(0);
        // schema should comply "fieldN"
        assertNotNull(record.getString("field0"));
        assertNotNull(record.getString("field1"));
    }

    @Test
    public void jsonBasicCase() {
        JsonConfiguration jsonConfiguration = new JsonConfiguration();
        dataSet.setFormat(FileFormat.JSON);
        dataSet.setJsonConfiguration(jsonConfiguration);
        dataSet.setBlobPath("demo_gen2/in/sample.json");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "AdlsGen2://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1, records.size());
        Record record = records.get(0);
        assertNotNull(record);
        assertEquals("Bonjour", record.getString("string1"));
        assertEquals("Olà", record.getString("string2"));
        assertEquals(1971, record.getLong("long"));
        assertEquals(19.71, record.getDouble("double"));
        assertFalse(record.getBoolean("false"));
        assertTrue(record.getBoolean("true"));
        assertEquals("prop1", record.getRecord("object").getString("prop1"));
        assertEquals(2, record.getRecord("object").getLong("prop2"));
        assertThat(record.getArray(Long.class, "intary"), contains(12l, 212l, 4343l, 545l));
        assertEquals(2, record.getArray(Record.class, "ary").size());
        record.getArray(Record.class, "ary").stream().map(record1 -> {
            assertTrue(record1.getString("id").contains("string id"));
            assertTrue(record1.getLong("cost") % 1024 == 0);
            return null;
        });
    }

    @Test
    public void jsonArrayCase() {
        JsonConfiguration jsonConfiguration = new JsonConfiguration();
        dataSet.setFormat(FileFormat.JSON);
        dataSet.setJsonConfiguration(jsonConfiguration);
        dataSet.setBlobPath("demo_gen2/in/sample-array.json");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "AdlsGen2://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(3, records.size());
        Record record = records.get(0);
        assertNotNull(record);
        assertEquals("Bonjour", record.getString("string1"));
        assertEquals("Olà", record.getString("string2"));
        assertEquals(1971, record.getLong("long"));
        assertEquals(19.71, record.getDouble("double"));
        assertFalse(record.getBoolean("false"));
        assertTrue(record.getBoolean("true"));
        assertEquals("prop1", record.getRecord("object").getString("prop1"));
        assertEquals(2, record.getRecord("object").getLong("prop2"));
        assertThat(record.getArray(Long.class, "intary"), contains(12l, 212l, 4343l, 545l));
        assertEquals(2, record.getArray(Record.class, "ary").size());
        record.getArray(Record.class, "ary").stream().map(record1 -> {
            assertTrue(record1.getString("id").contains("string id"));
            assertTrue(record1.getLong("cost") % 1024 == 0);
            return null;
        });
        record = records.get(2);
        assertNotNull(record);
        assertEquals("Bonjour3", record.getString("string1"));
        assertEquals("Olà3", record.getString("string2"));
        assertEquals(1973, record.getLong("long"));
        assertEquals(19.73, record.getDouble("double"));
        assertFalse(record.getBoolean("false"));
        assertTrue(record.getBoolean("true"));
        assertEquals("prop1", record.getRecord("object").getString("prop1"));
        assertEquals(2, record.getRecord("object").getLong("prop2"));
        assertThat(record.getArray(Long.class, "intary"), contains(12l, 212l, 4343l, 545l));
        assertEquals(2, record.getArray(Record.class, "ary").size());
        record.getArray(Record.class, "ary").stream().map(record1 -> {
            assertTrue(record1.getString("id").contains("string id"));
            assertTrue(record1.getLong("cost") % 1024 == 0);
            return null;
        });
    }

}
