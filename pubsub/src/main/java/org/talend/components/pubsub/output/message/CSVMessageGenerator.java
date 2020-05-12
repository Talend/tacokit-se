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
package org.talend.components.pubsub.output.message;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.service.PubSubConnectorException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordService;
import org.talend.sdk.component.api.service.record.RecordVisitor;
import org.talend.sdk.component.runtime.manager.service.RecordServiceImpl;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

@Slf4j
public class CSVMessageGenerator extends MessageGenerator {

    private char fieldDelimiter;

    protected static class CSVRecordVisitor implements RecordVisitor<String> {

        private StringBuilder csv = new StringBuilder();

        private RecordVisitor<String> doNothingVisitor = new RecordVisitor<String>() {
        };

        private char fieldDelimiter;

        CSVRecordVisitor(char fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
        }

        @Override
        public String get() {
            if (csv.charAt(csv.length() - 1) == fieldDelimiter) {
                // Remove last delimiter
                csv.setLength(csv.length() - 1);
            }

            return csv.toString();
        }

        @Override
        public void onBoolean(Schema.Entry entry, Optional<Boolean> optionalBoolean) {
            optionalBoolean.ifPresent(b -> csv.append(String.valueOf(b)));
            csv.append(fieldDelimiter);
        }

        @Override
        public void onDouble(Schema.Entry entry, OptionalDouble optionalDouble) {
            optionalDouble.ifPresent(d -> csv.append(String.valueOf(d)));
            csv.append(fieldDelimiter);
        }

        @Override
        public void onFloat(Schema.Entry entry, OptionalDouble optionalFloat) {
            optionalFloat.ifPresent(f -> csv.append(String.valueOf(f)));
            csv.append(fieldDelimiter);
        }

        @Override
        public void onInt(Schema.Entry entry, OptionalInt optionalInt) {
            optionalInt.ifPresent(i -> csv.append(String.valueOf(i)));
            csv.append(fieldDelimiter);
        }

        @Override
        public void onLong(Schema.Entry entry, OptionalLong optionalLong) {
            optionalLong.ifPresent(l -> csv.append(String.valueOf(l)));
            csv.append(fieldDelimiter);
        }

        @Override
        public void onString(Schema.Entry entry, Optional<String> string) {
            string.ifPresent(s -> csv.append(String.valueOf(s)));
            csv.append(fieldDelimiter);
        }

        @Override
        public void onBytes(Schema.Entry entry, Optional<byte[]> bytes) {
            onString(entry, Optional.of(Base64.getEncoder().encodeToString(bytes.orElse(new byte[] {}))));
        }

        @Override
        public void onDatetime(Schema.Entry entry, Optional<ZonedDateTime> dateTime) {
            onString(entry,
                    Optional.of(dateTime.map(dt -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss").format(dt)).orElse(null)));
        }

        @Override
        public RecordVisitor<String> onRecordArray(Schema.Entry entry, Optional<Collection<Record>> array) {
            return doNothingVisitor;
        }

        @Override
        public RecordVisitor<String> onRecord(Schema.Entry entry, Optional<Record> record) {
            return doNothingVisitor;
        }
    }

    @Override
    public void init(PubSubDataSet dataset) {
        this.fieldDelimiter = dataset.getFieldDelimiter() == PubSubDataSet.CSVDelimiter.OTHER ? dataset.getOtherDelimiter()
                : dataset.getFieldDelimiter().getValue();
    }

    @Override
    public PubsubMessage generateMessage(Record record) {
        try {
            String csv = getRecordService().visit(new CSVRecordVisitor(fieldDelimiter), record);
            return PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(csv)).build();
        } catch (Exception e) {
            log.error(getI18nMessage().errorWriteCSV(e.getMessage()), e);
            throw new PubSubConnectorException(getI18nMessage().errorWriteCSV(e.getMessage()));
        }
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return format == PubSubDataSet.ValueFormat.CSV;
    }
}
