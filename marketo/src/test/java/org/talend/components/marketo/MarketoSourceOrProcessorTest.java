// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo;

import static org.junit.Assert.assertTrue;

import javax.json.JsonObject;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.input.LeadSource;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.marketo")
class MarketoSourceOrProcessorTest extends MarketoBaseTest {

    Schema schema;

    IndexedRecord record;

    JsonObject json;

    MarketoSourceOrProcessor sop;

    private transient static final Logger LOG = LoggerFactory.getLogger(MarketoSourceOrProcessorTest.class);

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();

        sop = new LeadSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        schema = SchemaBuilder.record("record").fields().name("id").type().nullable().intType().noDefault().endRecord();
        record = new Record(schema);
        record.put(0, 12345);
        json = jsonFactory.createObjectBuilder().add("id", 9876).build();
    }

    @Test
    void toIndexedRecord() {
        IndexedRecord ir = sop.toIndexedRecord(json, schema);
        assertTrue(ir instanceof IndexedRecord);
        LOG.warn("[toIndexedRecord] IN:{}; OUT:{}.", json, ir);
    }

    @Test
    void toJsonObject() {
        JsonObject js = sop.toJson(record);
        assertTrue(js instanceof JsonObject);
        LOG.warn("[toJsonObject] IN:{}; OUT:{}.", record, js);
    }

}
