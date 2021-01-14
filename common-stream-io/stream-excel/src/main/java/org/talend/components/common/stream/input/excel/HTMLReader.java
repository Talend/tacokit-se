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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.talend.components.common.collections.IteratorComposer;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class HTMLReader implements FormatReader {

    private final HTMLToRecord toRecord;

    public HTMLReader(RecordBuilderFactory recordBuilderFactory) {
        this.toRecord = new HTMLToRecord(recordBuilderFactory);
    }

    @Override
    public Iterator<Record> read(InputStream input, ExcelConfiguration configuration) throws IOException {
        final String encoding = configuration.getEncoding().getEncoding();
        final Document document = Jsoup.parse(input, encoding, "");

        final Element table = document.body().getElementsByTag("table").first();
        final Elements elementTable = table.getElementsByTag("tbody").first().children();

        final Iterator<Element> rowIterator = elementTable.iterator();
        final Schema schema = this.toRecord.inferSchema(elementTable.get(0));
        if (elementTable.first().getElementsByTag("th").size() > 0) {
            // skip real header.
            rowIterator.next();
        }

        return IteratorComposer.of(rowIterator).map((Element element) -> this.toRecord.toRecord(schema, element)).build();
    }
}
