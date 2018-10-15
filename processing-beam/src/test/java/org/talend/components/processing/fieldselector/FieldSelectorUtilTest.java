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
package org.talend.components.processing.fieldselector;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Test;
import org.talend.components.processing.SampleAvpathSchemas;
import org.talend.daikon.exception.TalendRuntimeException;
import wandou.avpath.Evaluator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

public class FieldSelectorUtilTest {

    private final Schema inputSimpleSchema = SchemaBuilder.record("inputRow") //
            .fields() //
            .name("a").type().optional().stringType() //
            .name("b").type().optional().stringType() //
            .name("c").type().optional().stringType() //
            .endRecord();

    private final IndexedRecord inputHierarchical = SampleAvpathSchemas.SyntheticDatasets.getRandomRecord(new Random(0),
            SampleAvpathSchemas.SyntheticDatasets.RECORD_A);

    @Test
    public void testCanRetrieveMultipleElements() {
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a"));
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a.b"));
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a.b.c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[*].c"));
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a[1].c"));
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a[28].c"));
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a[-1].c"));
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a[-28].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[1:].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[:1].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[-28:].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[:-333].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[283:555555].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[-9999:-42].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[-1][283:555555].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[283:555555][-1].c"));
        assertFalse(FieldSelectorUtil.canRetrieveMultipleElements(".a[-1].a[1].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a{.b.c > 10}.c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a[-1]{.a > 28}.a[1].c"));
        assertTrue(FieldSelectorUtil.canRetrieveMultipleElements(".a..c"));
    }

    @Test
    public void testChangeAVPathToSchemaRetriever() {
        assertEquals(".a", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a"));
        assertEquals(".a.b", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a.b"));
        assertEquals(".a.b.c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a.b.c"));
        assertEquals(".a[*].c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a[].c"));
        assertEquals(".a[*].c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a[*].c"));
        assertEquals(".a[*].c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a[1].c"));
        assertEquals(".a[*].c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a[-1].c"));
        assertEquals(".a[*].c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a[1:].c"));
        assertEquals(".a[*].c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a[:1].c"));
        assertEquals(".a[*][*][*].c[*]", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a[1][2][*].c[3]"));
        assertEquals(".a.c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a{}.c"));
        assertEquals(".a.c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a{.b.c > 10}.c"));
        // the input syntax is not compatible with AVPath but whatever
        assertEquals(".a.c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a{.b.c > 10}{.b.c > 10}{}.c{.b.c > 10}"));
        assertEquals(".a..c", FieldSelectorUtil.changeAVPathToSchemaRetriever(".a..c"));
        assertEquals(".a.b[*].c[*][*].e.f",
                FieldSelectorUtil.changeAVPathToSchemaRetriever(".a{.b.c > 10}.b[-28].c[33:]{ .e.f == 28 }[*].e.f{.g < .h}"));
    }

    @Test
    public void testGenerateIndexedRecord() {
        Map<String, Object> inputMaps = new HashMap<>();
        inputMaps.put("a", "test1");
        inputMaps.put("c", "test3");
        inputMaps.put("b", "test2");
        IndexedRecord output = FieldSelectorUtil.generateIndexedRecord(inputMaps, inputSimpleSchema);
        // the order is the one defined on the schema
        assertEquals("test1", output.get(0));
        assertEquals("test2", output.get(1));
        assertEquals("test3", output.get(2));
    }

    @Test
    public void testGetInputFields() {
        assertEquals(0, FieldSelectorUtil.getInputFields(inputHierarchical, "NOTHING").size());

        assertEquals(1, (int) FieldSelectorUtil.getInputFields(inputHierarchical, "id").get(0).value());
        assertEquals(1, (int) FieldSelectorUtil.getInputFields(inputHierarchical, ".{.id == 1}.id").get(0).value());
        assertEquals(1, (int) FieldSelectorUtil.getInputFields(inputHierarchical, ".id").get(0).value());
        assertEquals(1, (int) FieldSelectorUtil.getInputFields(inputHierarchical, "a1.id").get(0).value());
        assertEquals(1, (int) FieldSelectorUtil.getInputFields(inputHierarchical, ".a1.id").get(0).value());
        assertEquals(5, (int) FieldSelectorUtil.getInputFields(inputHierarchical, "a1.a2.id").get(0).value());
        assertEquals(5, (int) FieldSelectorUtil.getInputFields(inputHierarchical, ".a1.a2.id").get(0).value());
        assertEquals("Q2G5V64PQQ", FieldSelectorUtil.getInputFields(inputHierarchical, "a1.a2.name").get(0).value());
        assertEquals("Q2G5V64PQQ", FieldSelectorUtil.getInputFields(inputHierarchical, ".a1.a2.name").get(0).value());
    }

    @Test(expected = TalendRuntimeException.class)
    public void testGetInputFieldsInvalid() throws TalendRuntimeException {
        FieldSelectorUtil.getInputFields(inputHierarchical, "NOPµf,sgldsf**///E");
    }

    @Test
    public void testExtractValuesFromContextSimpleValue() {

        List<Evaluator.Ctx> ctxList = FieldSelectorUtil.getInputFields(inputHierarchical, "id");
        Object id = FieldSelectorUtil.extractValuesFromContext(ctxList, "id");
        assertEquals(1, (int) id);
    }

    @Test
    public void testExtractValuesFromContextList() {
        List<Evaluator.Ctx> ctxList = FieldSelectorUtil.getInputFields(inputHierarchical, ".a1{.id == 1}.id");
        List<Object> idList = (List<Object>) FieldSelectorUtil.extractValuesFromContext(ctxList, ".a1{.id == 1}.id");
        assertEquals(1, idList.size());
        assertEquals(1, (int) idList.get(0));
    }
}
