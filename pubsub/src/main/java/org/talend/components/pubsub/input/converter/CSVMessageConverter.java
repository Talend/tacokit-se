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
package org.talend.components.pubsub.input.converter;

import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
public class CSVMessageConverter extends MessageConverter {

    public static final String FIELD_PREFIX = "field";

    public static final String FIELD_DEFAULT_VALUE = "";

    private CSVRecordConverter converter;

    @Override
    public void init(PubSubDataSet dataset) {
        char delimiter = dataset.getFieldDelimiter() == PubSubDataSet.CSVDelimiter.OTHER ? dataset.getOtherDelimiter()
                : dataset.getFieldDelimiter().getValue();
        converter = CSVRecordConverter.of(getRecordBuilderFactory(), delimiter);
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return format == PubSubDataSet.ValueFormat.CSV;
    }

    @Override
    public Record convertMessage(PubsubMessage message) {
        String messageContent = getMessageContentAsString(message);

        try {
            CSVParser parser = new CSVParser(new StringReader(messageContent), converter.getCsvFormat());
            List<CSVRecord> records = parser.getRecords();
            if (records.size() == 1) {
                return converter.toRecord(records.get(0));
            } else {
                log.error(getI18nMessage().errorBadCSV());
            }
        } catch (Exception e) {
            log.error(getI18nMessage().errorReadCSV(e.getMessage()));
            return null;
        }

        return null;
    }
}
