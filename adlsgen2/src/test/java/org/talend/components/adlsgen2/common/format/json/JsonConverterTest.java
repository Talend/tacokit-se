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
package org.talend.components.adlsgen2.common.format.json;

import java.io.FileInputStream;
import java.io.InputStreamReader;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.adlsgen2")
class JsonConverterTest extends AdlsGen2TestBase {

    private JsonConverter converter = JsonConverter.of();

    private JsonObject json;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();

        String sample = getClass().getResource("/common/format/json/sample.json").getPath();
        json = Json.createParser(new InputStreamReader(new FileInputStream(sample))).getObject();
    }

    @Test
    void inferSchema() {
        System.err.println(converter.inferSchema(json));
    }

    @Test
    void toRecord() {
    }

    @Test
    void fromRecord() {
    }

    @Test
    void readJsonSample() throws Exception {
        System.err.println(json);
        Record record = converter.toRecord(json);
        System.err.println(record);
    }

}
