/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.math.BigDecimal;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Before;
import org.junit.Test;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.avro.converter.IndexedRecordConverter;

/**
 *
 */
public class BulkResultAdapterFactoryTest {


    public static final Schema SCHEMA = SchemaBuilder.builder().record("Schema").fields() //
            .name("Id").prop(SchemaConstants.TALEND_COLUMN_IS_KEY, "true").type().stringType().noDefault() //
            .name("testString").type().stringType().noDefault() //
            .name("testBoolean").type().booleanType().noDefault() //
            .name("testByte").type(AvroUtils._byte()).noDefault() //
            .name("testBytes").type().bytesType().noDefault() //
            .name("testDate").prop(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd").type(AvroUtils._logicalTimestamp()).noDefault() //
            .name("testDateTime").prop(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd'T'HH:mm:ss'.000Z'").type(AvroUtils._logicalTimestamp()).noDefault() //
            .name("testDecimal").type(AvroUtils._decimal()).noDefault() //
            .name("testDouble").type().doubleType().noDefault() //
            .name("testFloat").type().floatType().noDefault() //
            .name("testInteger").type().intType().noDefault() //
            .name("testLong").type().longType().noDefault() //
            .name("testShort").type(AvroUtils._short()).noDefault() //
            .endRecord();

    private BulkResultAdapterFactory converter;

    @Before
    public void setUp() {
        converter = new BulkResultAdapterFactory();
    }

    @Test
    public void testConvertToAvro() {
        converter.setSchema(SCHEMA);

        assertNotNull(converter.getSchema());
        assertEquals(BulkResult.class, converter.getDatumClass());

        BulkResult result = new BulkResult();
        result.setValue("Id", "12345");
        result.setValue("testString", "This is String");
        result.setValue("testBoolean", "true");
        result.setValue("testByte", "18");
        result.setValue("testBytes", "This is bytes");
        result.setValue("testDate", "2018-07-25");
        result.setValue("testDateTime", "2018-07-25T10:10:10.000Z");
        result.setValue("testDecimal", "60000.0");
        result.setValue("testDouble", "5000.0");
        result.setValue("testFloat", "400.0");
        result.setValue("testInteger", "10000");
        result.setValue("testLong", "86400000");
        result.setValue("testShort", "259");

        IndexedRecord indexedRecord = converter.convertToAvro(result);
        assertNotNull(indexedRecord);
        assertNotNull(indexedRecord.getSchema());
        assertEquals(SCHEMA, indexedRecord.getSchema());

        assertEquals("12345", indexedRecord.get(0));
        assertEquals("This is String", indexedRecord.get(1));
        assertEquals(Boolean.TRUE, indexedRecord.get(2));
        assertEquals(Byte.valueOf("18"), indexedRecord.get(3));
        assertEquals("This is bytes", new String((byte[]) indexedRecord.get(4)));
        assertEquals(1532448000000L, indexedRecord.get(5));
        assertEquals(1532484610000L, indexedRecord.get(6));
        assertEquals(BigDecimal.valueOf(60000.0), indexedRecord.get(7));
        assertEquals(5000.0, indexedRecord.get(8));
        assertEquals(400.0f, indexedRecord.get(9));
        assertEquals(10000, indexedRecord.get(10));
        assertEquals(86400000L, indexedRecord.get(11));
        assertEquals(Short.valueOf("259"), indexedRecord.get(12));
    }

    @Test(expected = IndexedRecordConverter.UnmodifiableAdapterException.class)
    public void testConvertToDatum() {
        converter.setSchema(SCHEMA);
        converter.convertToDatum(new GenericData.Record(converter.getSchema()));
    }

    @Test(expected = IndexedRecordConverter.UnmodifiableAdapterException.class)
    public void testIndexedRecordUnmodifiable() {
        converter.setSchema(SCHEMA);

        BulkResult result = new BulkResult();
        result.setValue("Id", "12345");
        result.setValue("Name", "Qwerty");
        result.setValue("FieldX", "42");
        result.setValue("FieldY", "true");

        IndexedRecord indexedRecord = converter.convertToAvro(result);
        indexedRecord.put(1, "Asdfgh");
    }
}
