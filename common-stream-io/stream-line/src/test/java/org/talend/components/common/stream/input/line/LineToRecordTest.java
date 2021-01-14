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
package org.talend.components.common.stream.input.line;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class LineToRecordTest {

    RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    @Test
    void translate() {
        final List<String> testListe = new ArrayList<>();
        final LineSplitter splitter = (String line) -> testListe;

        LineToRecord toRecord = new LineToRecord(factory, splitter);

        testListe.clear();
        testListe.add("Hello");
        testListe.add("World");
        final Record record = toRecord.translate("");
        Assertions.assertEquals("Hello", record.getString("field_1"));
        Assertions.assertEquals("World", record.getString("field_2"));
    }

    @Test
    void translateWithHeaders() {
        final LineSplitter splitter = (String line) -> Arrays.asList(line.split(";"));

        LineToRecord toRecord = new LineToRecord(factory, splitter);
        toRecord.withHeaders("greetings;who");

        final Record record = toRecord.translate("Hello;World");
        Assertions.assertEquals("Hello", record.getString("greetings"));
        Assertions.assertEquals("World", record.getString("who"));
    }
}