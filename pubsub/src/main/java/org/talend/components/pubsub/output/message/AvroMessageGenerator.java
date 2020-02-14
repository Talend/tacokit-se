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
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.service.PubSubConnectorException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;

@Slf4j
public class AvroMessageGenerator extends MessageGenerator {

    private RecordWriterSupplier recordWriterSupplier;

    private AvroConfiguration avroConfiguration;

    @Override
    public void init(PubSubDataSet dataset) {
        recordWriterSupplier = getIoRepository().findWriter(AvroConfiguration.class);
        avroConfiguration = new AvroConfiguration();
        avroConfiguration.setAttachSchema(false);
        avroConfiguration.setAvroSchema(dataset.getAvroSchema());
    }

    @Override
    public PubsubMessage generateMessage(Record record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            RecordWriter recordWriter = recordWriterSupplier.getWriter(() -> out, avroConfiguration);
            recordWriter.init(avroConfiguration);
            recordWriter.add(record);
            recordWriter.flush();
            recordWriter.end();
            recordWriter.close();
            PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFrom(out.toByteArray())).build();
            return message;
        } catch (Exception e) {
            log.error(getI18nMessage().errorWriteAvro(e.getMessage()), e);
            return null;
        }
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return format == PubSubDataSet.ValueFormat.AVRO;
    }
}
