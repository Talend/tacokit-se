package org.talend.components.processing.flatten;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.talend.daikon.exception.TalendRuntimeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlattenUtilsTest {

    private final Schema inputSchemaL = SchemaBuilder.record("inputRowL") //
            .fields() //
            .name("l").type().optional().stringType() //
            .endRecord();

    private final Schema inputSchemaListOfL = SchemaBuilder.array().items(inputSchemaL);

    private final Schema inputSchemaJK = SchemaBuilder.record("inputRowJK") //
            .fields() //
            .name("j").type(inputSchemaListOfL).noDefault() //
            .name("k").type().optional().stringType() //
            .endRecord();

    private final Schema inputSchemaDE = SchemaBuilder.record("inputRowDE") //
            .fields() //
            .name("d").type(inputSchemaJK).noDefault() //
            .name("e").type().optional().stringType() //
            .endRecord();

    private final Schema inputSchemaHI = SchemaBuilder.record("inputRowHI") //
            .fields() //
            .name("h").type().optional().stringType() //
            .name("i").type().optional().stringType() //
            .endRecord();

    private final Schema inputSchemaListOfHI = SchemaBuilder.array().items(inputSchemaHI);

    private final Schema inputSchemaFG = SchemaBuilder.record("inputRowFG") //
            .fields() //
            .name("f").type().optional().stringType() //
            .name("g").type(inputSchemaListOfHI).noDefault() //
            .endRecord();

    private final Schema inputSchemaXY = SchemaBuilder.record("inputRowXY") //
            .fields() //
            .name("x").type().optional().stringType() //
            .name("y").type(inputSchemaDE).noDefault() //
            .endRecord();

    private final Schema inputSchemaListM = SchemaBuilder.array().items().stringType();

    private final Schema inputParentSchema = SchemaBuilder.record("inputParentRow") //
            .fields() //
            .name("a").type().optional().stringType() //
            .name("b").type(inputSchemaXY).noDefault() //
            .name("c").type(inputSchemaFG).noDefault() //
            .name("m").type(inputSchemaListM).noDefault() //
            .endRecord();

    /**
     * {"l":"l1"}
     */
    private final GenericRecord inputRecordL1 = new GenericRecordBuilder(inputSchemaL) //
            .set("l", "l1") //
            .build();

    /**
     * {"l":"l2"}
     */
    private final GenericRecord inputRecordL2 = new GenericRecordBuilder(inputSchemaL) //
            .set("l", "l2") //
            .build();

    /**
     * [{"l":"l1"},{"l":"l2"}]
     */
    private final List<GenericRecord> listInputRecordL = Arrays.asList(inputRecordL1, inputRecordL2);

    /**
     * {"j": [{"l":"l1"},{"l":"l2"}], "k": "k1;k2"}
     */
    private final GenericRecord inputRecordJK = new GenericRecordBuilder(inputSchemaJK) //
            .set("j", listInputRecordL) //
            .set("k", "k1;k2") //
            .build();

    /**
     * {"d": {"j": [{"l":"l1"},{"l":"l2"}], "k": "k1;k2"}, "e": "e"}
     */
    private final GenericRecord inputRecordDE = new GenericRecordBuilder(inputSchemaDE) //
            .set("d", inputRecordJK) //
            .set("e", "e") //
            .build();

    /**
     * {"h": "h1", "i": "i2"}
     */
    private final GenericRecord inputRecordHI1 = new GenericRecordBuilder(inputSchemaHI) //
            .set("h", "h1") //
            .set("i", "i2") //
            .build();

    /**
     * {"h": "h2", "i": "i1"}
     */
    private final GenericRecord inputRecordHI2 = new GenericRecordBuilder(inputSchemaHI) //
            .set("h", "h2") //
            .set("i", "i1") //
            .build();

    /**
     * [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]
     */
    private final List<GenericRecord> listInputRecordG = Arrays.asList(inputRecordHI1, inputRecordHI2);

    /**
     * {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}
     */
    private final GenericRecord inputRecordFG = new GenericRecordBuilder(inputSchemaFG) //
            .set("f", "f") //
            .set("g", listInputRecordG) // inputRecordHI
            .build();

    /**
     * {"x": "x1;x2", "y": {"d": {"j": [{"l":"l1"},{"l":"l2"}], "k": "k1;k2"}, "e": "e"}}
     */
    private final GenericRecord inputRecordXY = new GenericRecordBuilder(inputSchemaXY) //
            .set("x", "x1;x2") //
            .set("y", inputRecordDE) // listDE
            .build();

    /**
     * ["m1", "m2", "m3"]
     */
    private final List<String> listInputRecordM = Arrays.asList("m1", "m2", "m3");

    /**
     * { "a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l":"l1"},{"l":"l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}
     */
    private final GenericRecord inputParentRecord = new GenericRecordBuilder(inputParentSchema) //
            .set("a", "aaa") //
            .set("b", inputRecordXY) //
            .set("c", inputRecordFG) //
            .set("m", listInputRecordM) //
            .build();

    private final Schema listSchemas = SchemaBuilder.array().items(inputSchemaListM);

    private final List<List<String>> listRecords = Arrays.asList(listInputRecordM);

    private final Schema listOfListSchema = SchemaBuilder.record("listOfListRow") //
            .fields() //
            .name("parentList").type(listSchemas).noDefault() //
            .endRecord();

    private final GenericRecord listOfListRecord = new GenericRecordBuilder(listOfListSchema) //
            .set("parentList", listRecords) //
            .build();

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * Get and check sub-fields of the simple field `b.y.d.k`
     *
     * Expected sub-fields of the simple field `b.y.d.k`: "k1;k2"
     */
    @Test
    public void testGetInputFieldsSimple() {
        List<Object> list = FlattenUtils.getInputFields(inputParentRecord, "b.y.d.k");
        Assert.assertEquals(inputRecordJK.get("k").toString(), list.get(0).toString());
    }

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * Get and check sub-fields of the array field `b.y.d.j`
     *
     * Expected sub-fields of the array field `b.y.d.j`: {"l": "l1"}, {"l": "l1"}
     */
    @Test
    public void testGetInputFieldsArray() {
        List<Object> list = FlattenUtils.getInputFields(inputParentRecord, "b.y.d.j");
        Assert.assertEquals(inputRecordL1.toString(), list.get(0).toString());
        Assert.assertEquals(inputRecordL2.toString(), list.get(1).toString());
    }

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * Get and check sub-fields of the complex field `b.y.d`
     *
     * Expected sub-fields of the complex field `b.y.d`: {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}
     */
    @Test
    public void testGetInputFieldsComplex() {

        List<Object> list = FlattenUtils.getInputFields(inputParentRecord, "b.y.d");
        Assert.assertEquals(inputRecordJK.toString(), list.get(0).toString());
    }

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * Get sub-fields of a field not present in the input parent record will throw TalendRuntimeException.
     */
    @Test(expected = TalendRuntimeException.class)
    public void testGetInputFieldsException() {
        FlattenUtils.getInputFields(inputParentRecord, "b.y.z");
    }

    /**
     * retrieve a valid record
     */
    @Test
    public void testGetChildSchemaAsRecord_valid() {
        Assert.assertEquals(inputSchemaXY,
                FlattenUtils.getChildSchemaAsRecord(inputParentRecord.getSchema(), inputParentRecord.getSchema().getField("b")));
    }

    /**
     * retrieve a invalid record => "m" is a list. As a resutl throw and TalendRuntimeException
     */
    @Test(expected = TalendRuntimeException.class)
    public void testGetChildSchemaAsRecord_invalid() {
        FlattenUtils.getChildSchemaAsRecord(inputParentRecord.getSchema(), inputParentRecord.getSchema().getField("m"));
    }

    /**
     * retrieve a valid record
     */
    @Test
    public void testGetChildSchemaOfListAsRecord_valid() {
        Assert.assertEquals(inputSchemaHI,
                FlattenUtils.getChildSchemaOfListAsRecord(inputRecordFG.getSchema(), inputRecordFG.getSchema().getField("g")));
    }

    /**
     * retrieve a invalid record => "b" is a not a list. As a resutl throw and TalendRuntimeException
     */
    @Test(expected = TalendRuntimeException.class)
    public void testGetChildSchemaOfLiswtAsRecord_invalid() {
        FlattenUtils.getChildSchemaOfListAsRecord(listOfListRecord.getSchema(),
                listOfListRecord.getSchema().getField("parentList"));
    }

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * will throw TalendRuntimeException. @TODO
     */
    @Test(expected = TalendRuntimeException.class)
    public void testGenerateNormalizedRecordException() {

        Schema wrongSchema = SchemaBuilder.record("inputParentRow") //
                .fields() //
                .name("a").type().optional().stringType() //
                .name("q").type(inputSchemaXY).noDefault() //
                .name("c").type(inputSchemaFG).noDefault() //
                .name("m").type(inputSchemaListM).noDefault() //
                .endRecord();

        String[] path = { "b", "y", "d", "k" };

        Schema schema = FlattenUtils.transformSchema(inputParentRecord.getSchema(), path, 0);

        FlattenUtils.generateNormalizedRecord(inputParentRecord, wrongSchema, schema, path, 0, null);
    }

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * Normalize simple field: `b.y.d.k`
     *
     * Expected normalized results of the field `b.y.d.k`:
     *
     * outputRecord1: {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1"}, "e":
     * "e"}}, "c": {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}
     *
     * outputRecord2: {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k2"}, "e":
     * "e"}}, "c": {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}
     *
     */
    @Test
    public void testGenerateNormalizedRecordSimple() {

        String[] path = { "b", "y", "d", "k" };

        Schema schema = FlattenUtils.transformSchema(inputParentRecord.getSchema(), path, 0);

        List<Object> normalizedFields = FlattenUtils.getInputFields(inputParentRecord, "b.y.d.k");

        normalizedFields = FlattenUtils.delimit(String.valueOf(normalizedFields.get(0)), ";", true, true);

        GenericRecord outputRecord1 = FlattenUtils.generateNormalizedRecord(inputParentRecord, inputParentRecord.getSchema(),
                schema, path, 0, normalizedFields.get(0));
        GenericRecord outputRecord2 = FlattenUtils.generateNormalizedRecord(inputParentRecord, inputParentRecord.getSchema(),
                schema, path, 0, normalizedFields.get(1));

        GenericRecord expectedRecordJK1 = new GenericRecordBuilder(inputSchemaJK) //
                .set("j", listInputRecordL) //
                .set("k", "k1") //
                .build();
        GenericRecord expectedRecordDE1 = new GenericRecordBuilder(inputSchemaDE) //
                .set("d", expectedRecordJK1) //
                .set("e", "e") //
                .build();
        GenericRecord expectedRecordXY1 = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x1;x2") //
                .set("y", expectedRecordDE1) //
                .build();
        GenericRecord expectedParentRecordK1 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordXY1) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        GenericRecord expectedRecordJK2 = new GenericRecordBuilder(inputSchemaJK) //
                .set("j", listInputRecordL) //
                .set("k", "k2") //
                .build();
        GenericRecord expectedRecordDE2 = new GenericRecordBuilder(inputSchemaDE) //
                .set("d", expectedRecordJK2) //
                .set("e", "e") //
                .build();
        GenericRecord expectedRecordXY2 = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x1;x2") //
                .set("y", expectedRecordDE2) //
                .build();
        GenericRecord expectedParentRecordK2 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordXY2) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        Assert.assertEquals(expectedParentRecordK1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordK1.getSchema().toString(), outputRecord1.getSchema().toString());

        Assert.assertEquals(expectedParentRecordK2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordK2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * Normalize simple field: `c.g`
     *
     * The schema of g must change from a list to a simple object. Expected normalized results of the field `c.g`:
     *
     * outputRecord1: {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e":
     * "e"}}, "c": {"f": "f", "g": {"h": "h1", "i": "i2"}}, "m": ["m1", "m2", "m3"]}
     *
     * outputRecord2: {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e":
     * "e"}}, "c": {"f": "f", "g": {"h": "h2", "i": "i1"}}, "m": ["m1", "m2", "m3"]}
     *
     */
    @Test
    public void testGenerateNormalizedRecordArray() {

        String[] path = { "c", "g" };

        Schema schema = FlattenUtils.transformSchema(inputParentRecord.getSchema(), path, 0);

        List<Object> normalizedFields = FlattenUtils.getInputFields(inputParentRecord, "c.g");

        GenericRecord outputRecord1 = FlattenUtils.generateNormalizedRecord(inputParentRecord, inputParentRecord.getSchema(),
                schema, path, 0, normalizedFields.get(0));
        GenericRecord outputRecord2 = FlattenUtils.generateNormalizedRecord(inputParentRecord, inputParentRecord.getSchema(),
                schema, path, 0, normalizedFields.get(1));

        Schema expectedSchemaHI = SchemaBuilder.record("inputRowHI") //
                .fields() //
                .name("h").type().optional().stringType() //
                .name("i").type().optional().stringType() //
                .endRecord();

        Schema expectedSchemaFG = SchemaBuilder.record("inputRowFG") //
                .fields() //
                .name("f").type().optional().stringType() //
                .name("g").type(expectedSchemaHI).noDefault() //
                .endRecord();

        Schema expectedParentSchema = SchemaBuilder.record("inputParentRow") //
                .fields() //
                .name("a").type().optional().stringType() //
                .name("b").type(inputSchemaXY).noDefault() //
                .name("c").type(expectedSchemaFG).noDefault() //
                .name("m").type(inputSchemaListM).noDefault() //
                .endRecord();

        GenericRecord expectedRecordFG1 = new GenericRecordBuilder(expectedSchemaFG) //
                .set("f", "f") //
                .set("g", inputRecordHI1) //
                .build();
        GenericRecord expectedParentRecordG1 = new GenericRecordBuilder(expectedParentSchema) //
                .set("a", "aaa") //
                .set("b", inputRecordXY) //
                .set("c", expectedRecordFG1) //
                .set("m", listInputRecordM) //
                .build();

        GenericRecord expectedRecordFG2 = new GenericRecordBuilder(expectedSchemaFG) //
                .set("f", "f") //
                .set("g", inputRecordHI2) //
                .build();
        GenericRecord expectedParentRecordG2 = new GenericRecordBuilder(expectedParentSchema) //
                .set("a", "aaa") //
                .set("b", inputRecordXY) //
                .set("c", expectedRecordFG2) //
                .set("m", listInputRecordM) //
                .build();

        Assert.assertEquals(expectedParentRecordG1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordG1.getSchema().toString(), outputRecord1.getSchema().toString());

        Assert.assertEquals(expectedParentRecordG2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordG2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link @inputParentRecord}
     *
     * Normalize complex field: `b.y`
     *
     * Expected: no change
     *
     */
    @Test
    public void testGenerateNormalizedRecordComplex() {

        String[] path = { "b", "y" };

        Schema schema = FlattenUtils.transformSchema(inputParentRecord.getSchema(), path, 0);

        List<Object> normalizedFields = FlattenUtils.getInputFields(inputParentRecord, "b.y");

        GenericRecord outputRecord1 = FlattenUtils.generateNormalizedRecord(inputParentRecord, inputParentRecord.getSchema(),
                schema, path, 0, normalizedFields.get(0));

        Assert.assertEquals(inputParentRecord.toString(), outputRecord1.toString());
        Assert.assertEquals(inputParentRecord.getSchema().toString(), outputRecord1.getSchema().toString());
    }

    /**
     * Input schema: {@link @inputParentSchema}
     *
     * The field `c.g` is an array.
     *
     * Expected schema: the schema of the field `c.g` should be modified to a {@link @inputSchemaHI}
     */
    @Test
    public void testTransformSchema() {

        String[] path = { "c", "g" };
        Schema newSchema = FlattenUtils.transformSchema(inputParentSchema, path, 0);

        Schema expectedSchemaFG = SchemaBuilder.record("inputRowFG") //
                .fields() //
                .name("f").type().optional().stringType() //
                .name("g").type(inputSchemaHI).noDefault() //
                .endRecord();
        Schema expectedParentSchema = SchemaBuilder.record("inputParentRow") //
                .fields() //
                .name("a").type().optional().stringType() //
                .name("b").type(inputSchemaXY).noDefault() //
                .name("c").type(expectedSchemaFG).noDefault() //
                .name("m").type(inputSchemaListM).noDefault() //
                .endRecord();

        Assert.assertEquals(newSchema.toString(), expectedParentSchema.toString());
    }

    /**
     * Check if the duplicated record is the same than the input record.
     */
    @Test
    public void testDuplicateRecord() {

        GenericRecord duplicateRecord = FlattenUtils.duplicateRecord((GenericRecord) inputRecordXY.get("y"),
                inputSchemaXY.getField("y").schema(), inputSchemaXY.getField("y").schema());

        GenericRecord expectedRecord = (GenericRecord) inputRecordXY.get("y");

        Assert.assertEquals(duplicateRecord.toString(), expectedRecord.toString());
        Assert.assertEquals(duplicateRecord.getSchema().toString(), expectedRecord.getSchema().toString());
    }

    /**
     * Check if the list contains a simple field.
     */
    @Test
    public void testIsSimpleField() {

        List<Object> list = new ArrayList<Object>();
        list.add(0, "a1;a2");

        Assert.assertEquals(FlattenUtils.isSimpleField(list), true);
    }

    /**
     * Splits toSplit variable around matches of ";"
     *
     * Expected results of split: {"a1", "a2", "a3"}
     */
    @Test
    public void testDelimit() {

        String toSplit = "a1;a2;a3";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", false, false);
        Assert.assertEquals(listSplit.get(0).toString(), "a1");
        Assert.assertEquals(listSplit.get(1).toString(), "a2");
        Assert.assertEquals(listSplit.get(2).toString(), "a3");
    }

    @Test
    public void testDTESwithCompleteString() {

        String toSplit = "a1;a2;a3";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", true, false);
        Assert.assertEquals("a1", listSplit.get(0).toString());
        Assert.assertEquals("a2", listSplit.get(1).toString());
        Assert.assertEquals("a3", listSplit.get(2).toString());
    }

    @Test
    public void testDTESwithEmptyTrailingValues() {

        String toSplit = "a1;a2;a3;;";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", true, false);
        Assert.assertEquals("a1", listSplit.get(0).toString());
        Assert.assertEquals("a2", listSplit.get(1).toString());
        Assert.assertEquals("a3", listSplit.get(2).toString());
    }

    @Test
    public void testwoDTESwithEmptyTrailingValues() {

        String toSplit = "a1;a2;a3;;a4";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", false, false);
        Assert.assertEquals("a1", listSplit.get(0).toString());
        Assert.assertEquals("a2", listSplit.get(1).toString());
        Assert.assertEquals("a3", listSplit.get(2).toString());
        Assert.assertEquals("", listSplit.get(3).toString());
        Assert.assertEquals("a4", listSplit.get(4).toString());
    }

    @Test
    public void testDTESwithEmptyTrailingValuesInTheMiddle() {

        String toSplit = "a1;a2;a3;;a4";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", true, false);
        Assert.assertEquals("a1", listSplit.get(0).toString());
        Assert.assertEquals("a2", listSplit.get(1).toString());
        Assert.assertEquals("a3", listSplit.get(2).toString());
        Assert.assertEquals("", listSplit.get(3).toString());
        Assert.assertEquals("a4", listSplit.get(4).toString());
    }

    @Test
    public void testwoDTESwithEmptyTrailingValuesInTheEnd() {

        String toSplit = "a1;a2;a3;;";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", false, false);
        Assert.assertEquals("a1", listSplit.get(0).toString());
        Assert.assertEquals("a2", listSplit.get(1).toString());
        Assert.assertEquals("a3", listSplit.get(2).toString());
        Assert.assertEquals("", listSplit.get(3).toString());
        Assert.assertEquals("", listSplit.get(4).toString());
        Assert.assertEquals(5, listSplit.size());
    }

    @Test
    public void testDTESwithEmptyTrailingValuesInTheEnd() {

        String toSplit = "a1;a2;a3;;";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", true, false);
        Assert.assertEquals("a1", listSplit.get(0).toString());
        Assert.assertEquals("a2", listSplit.get(1).toString());
        Assert.assertEquals("a3", listSplit.get(2).toString());
        Assert.assertEquals(3, listSplit.size());
    }

    /**
     * Splits toSplit variable around matches of ";" with leading and trailing white space removed.
     * 
     * Expected results of split: {"a1", "a2", "a3"}
     */
    @Test
    public void testDelimitIsTrim() {

        String toSplit = " a1 ;  a2  ;   a3   ";
        List<Object> listSplit = FlattenUtils.delimit(toSplit, ";", false, true);
        Assert.assertEquals(listSplit.get(0).toString(), "a1");
        Assert.assertEquals(listSplit.get(1).toString(), "a2");
        Assert.assertEquals(listSplit.get(2).toString(), "a3");
    }
}
