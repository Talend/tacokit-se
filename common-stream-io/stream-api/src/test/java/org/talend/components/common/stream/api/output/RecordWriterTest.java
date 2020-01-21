package org.talend.components.common.stream.api.output;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.record.RecordImpl;

import static org.junit.jupiter.api.Assertions.*;

class RecordWriterTest {

    private static final String recordString = "unusefull tests datas [...............]\n";

    @Test
    void writeOutputStream() throws IOException {
        final RecordWriter writer = new RecordWriter(this::serialize);
        final  ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.write(out, (Record) null);

        String res = out.toString();
        Assertions.assertEquals(RecordWriterTest.recordString, res);
    }

    @Test
    void testCollectionOutputStream() throws IOException {
        final RecordWriter writer = new RecordWriter(this::serialize);
        final List<Record> records = Arrays.asList((Record) null, (Record) null, (Record) null);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.write(out, records);

        String res = out.toString();
        Assertions.assertEquals(res.length(), RecordWriterTest.recordString.length()*3);
    }

    @Test
    void testWriteChannel() throws IOException {
        final RecordWriter writer = new RecordWriter(this::serialize);
        TestWritableChannel out = new TestWritableChannel();
        writer.write(out, (Record) null);

        String res = out.getContent();
        Assertions.assertEquals(RecordWriterTest.recordString, res);
    }

    @Test
    void testWriteCollectionChannel() throws IOException {
        final List<Record> records = new ArrayList<>(1000); // sure to over default limit.
        for (int i = 0; i < 1000; i++) {
            records.add(null);
        }

        final RecordWriter writer = new RecordWriter(this::serialize);
        TestWritableChannel out = new TestWritableChannel();
        writer.write(out, records);
        String res = out.getContent();
        Assertions.assertEquals(res.length(), RecordWriterTest.recordString.length()*1000);
    }



    private byte[] serialize(Record record) {
        return RecordWriterTest.recordString.getBytes(Charset.defaultCharset());
    }

    static class TestWritableChannel implements WritableByteChannel {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public int write(ByteBuffer src) throws IOException {
            final byte[] array = src.array();
            out.write(array);
            return array.length;
        }

        public String getContent() {
            return out.toString();
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}