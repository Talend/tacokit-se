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
package org.talend.components.pubsub;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Assertions;
import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.components.pubsub.service.PubSubService;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Slf4j
public final class PubSubTestUtil {

    public static String GOOGLE_APPLICATION_CREDENTIALS;

    public static String GOOGLE_PROJECT;

    static {
        GOOGLE_APPLICATION_CREDENTIALS = Optional.ofNullable(System.getProperty("GOOGLE_APPLICATION_CREDENTIALS"))
                .orElseGet(() -> Optional.ofNullable(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
                        .orElseThrow(() -> new RuntimeException("GOOGLE_APPLICATION_CREDENTIALS not set")));

        GOOGLE_PROJECT = Optional.ofNullable(System.getProperty("GOOGLE_PROJECT")).orElse("engineering-152721");
        log.info("Using " + GOOGLE_APPLICATION_CREDENTIALS + " as Google credentials file");
    }

    public static PubSubDataStore getDataStore() {
        String jsonCredentials = "";
        try (FileInputStream in = new FileInputStream(GOOGLE_APPLICATION_CREDENTIALS);
                BufferedInputStream bIn = new BufferedInputStream(in)) {
            byte[] buffer = new byte[1024];
            int read = 0;
            while ((read = bIn.read(buffer)) > 0) {
                jsonCredentials += new String(buffer, 0, read, StandardCharsets.UTF_8);
            }
            jsonCredentials = jsonCredentials.replace("\n", " ").trim();
        } catch (IOException ioe) {
            Assertions.fail(ioe);
        }

        PubSubDataStore dataStore = new PubSubDataStore();
        dataStore.setProjectName(GOOGLE_PROJECT);
        dataStore.setJsonCredentials(jsonCredentials);

        return dataStore;
    }

    public static Schema getAvroSchema() {
        return SchemaBuilder.builder().record("testRecord").namespace("org.talend.test").fields().requiredInt("ID")
                .requiredString("content").endRecord();
    }

    public static String getAvroSchemaString() {
        return getAvroSchema().toString(true);
    }

    public static void sendAvroRecord(PubSubService service, String topic) {
        Publisher publisher = service.createPublisher(getDataStore(), topic);

        try {
            GenericRecord record = new GenericData.Record(getAvroSchema());
            record.put("ID", 42);
            record.put("content", "This is a test");
            log.debug(record.toString());

            SpecificDatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(record.getSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            out.close();

            log.debug(new String(out.toByteArray(), "ISO-8859-15"));

            PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFrom(out.toByteArray())).build();
            ApiFuture<String> future = publisher.publish(message);
            while (!future.isDone()) {
                Thread.sleep(500);
            }
            log.debug("Message sent : " + future.get());
        } catch (Exception e) {
            log.error("Error sending record", e);
        } finally {
            publisher.shutdown();
        }

    }
}
