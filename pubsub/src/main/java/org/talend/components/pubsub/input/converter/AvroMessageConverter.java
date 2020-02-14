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
package org.talend.components.pubsub.input.converter;

import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.record.Record;

@Slf4j
public class AvroMessageConverter extends MessageConverter {

    private Schema avroSchema;

    @Override
    public void init(PubSubDataSet dataset) {
        String avroSchemaString = dataset.getAvroSchema();

        if (avroSchemaString == null || "".equals(avroSchemaString.trim())) {
            throw new RuntimeException(getI18nMessage().avroSchemaRequired());
        }
        try {
            Schema.Parser schemaParser = new Schema.Parser();
            avroSchema = schemaParser.parse(avroSchemaString);
        } catch (Exception e) {
            throw new RuntimeException(getI18nMessage().avroSchemaInvalid(e.getMessage()));
        }
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return format == PubSubDataSet.ValueFormat.AVRO;
    }

    @Override
    public Record convertMessage(PubsubMessage message) {
        Record record = null;
        Decoder decoder = DecoderFactory.get().binaryDecoder(getMessageContentAsBytes(message), null);
        DatumReader<GenericRecord> recordDatumReader = new GenericDatumReader<>(avroSchema);
        try {
            GenericRecord gr = recordDatumReader.read(null, decoder);
            record = AvroRecordConverter.of(getRecordBuilderFactory()).toRecord(gr);
        } catch (Exception e) {
            log.error(getI18nMessage().errorReadAVRO(e.getMessage()));
        }

        return record;
    }
}
