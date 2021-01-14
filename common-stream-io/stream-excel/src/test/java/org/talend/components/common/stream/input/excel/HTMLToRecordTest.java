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
package org.talend.components.common.stream.input.excel;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class HTMLToRecordTest {

    @Test
    void inferSchema() throws IOException {
        Document doc = getSource();
        final Element table = doc.body().getElementsByTag("table").first();
        final Elements elementTable = table.getElementsByTag("tbody").first().children();

        HTMLToRecord toRecord = new HTMLToRecord(new RecordBuilderFactoryImpl("test"));

        final Schema schema = toRecord.inferSchema(elementTable.first());
        Assertions.assertEquals("Account_Owner", schema.getEntries().get(0).getName());

        Record rec1 = toRecord.toRecord(schema, elementTable.get(1));
        Assertions.assertNotNull(rec1);
        Assertions.assertEquals("compqa talend", rec1.getString("Account_Owner"));
    }

    Document getSource() throws IOException {
        URL res = Thread.currentThread().getContextClassLoader().getResource("./html/HTML-utf8.html");
        return Jsoup.parse(new File(res.getPath()), Charset.defaultCharset().name());
    }
}