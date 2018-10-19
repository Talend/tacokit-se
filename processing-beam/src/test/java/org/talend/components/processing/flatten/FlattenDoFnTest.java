package org.talend.components.processing.flatten;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.Test;
import org.talend.daikon.exception.TalendRuntimeException;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

public class FlattenDoFnTest {

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

    private FlattenConfiguration getFlattenConfigurationWithTrimAndTrailing() {
        FlattenConfiguration flattenConfiguration = new FlattenConfiguration();
        flattenConfiguration.setTrim(true);
        flattenConfiguration.setDiscardTrailingEmptyStr(true);
        return flattenConfiguration;
    }

    private FlattenConfiguration getDefaultFlattenConfiguration() {
        return new FlattenConfiguration();
    }

    /**
     * Invalid path to normalize => throw error
     */
    @Test
    public void testNormalize_nothing() throws Exception {
        FlattenConfiguration configuration = getFlattenConfigurationWithTrimAndTrailing();
        configuration.setColumnToFlatten(null);

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(0, outputs.size());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `a`
     *
     * Expected normalized results of the field `a`:
     *
     * {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c":
     * {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeSimpleFields_a() throws Exception {
        FlattenConfiguration configuration = getFlattenConfigurationWithTrimAndTrailing();
        configuration.setColumnToFlatten("a");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(1, outputs.size());
        GenericRecord outputRecord = (GenericRecord) outputs.get(0);
        Assert.assertEquals(inputParentRecord.toString(), outputRecord.toString());
        Assert.assertEquals(inputParentRecord.getSchema().toString(), outputRecord.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `.a`
     *
     * Expected normalized results of the field `a`:
     *
     * {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c":
     * {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeSimpleFields_a_withDot() throws Exception {
        FlattenConfiguration configuration = getFlattenConfigurationWithTrimAndTrailing();
        configuration.setColumnToFlatten(".a");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(1, outputs.size());
        GenericRecord outputRecord = (GenericRecord) outputs.get(0);
        Assert.assertEquals(inputParentRecord.toString(), outputRecord.toString());
        Assert.assertEquals(inputParentRecord.getSchema().toString(), outputRecord.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `b.x`
     *
     * Expected normalized results of the field `b.x`:
     *
     * [{"a": "aaa", "b": {"x": "x1", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]},
     * 
     * {"a": "aaa", "b": {"x": "x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}]
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeSimpleFields_bx() throws Exception {
        FlattenConfiguration configuration = getFlattenConfigurationWithTrimAndTrailing();
        configuration.setColumnToFlatten("b.x");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(2, outputs.size());

        GenericRecord expectedRecordX1Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x1") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedRecordX2Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x2") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedParentRecordX1 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX1Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();
        GenericRecord expectedParentRecordX2 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX2Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        Assert.assertEquals(expectedParentRecordX1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordX1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordX2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordX2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `.b.x`
     *
     * Expected normalized results of the field `b.x`:
     *
     * [{"a": "aaa", "b": {"x": "x1", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]},
     * 
     * {"a": "aaa", "b": {"x": "x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}]
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeSimpleFields_bx_withDot() throws Exception {
        FlattenConfiguration configuration = getFlattenConfigurationWithTrimAndTrailing();
        configuration.setColumnToFlatten(".b.x");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(2, outputs.size());

        GenericRecord expectedRecordX1Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x1") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedRecordX2Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x2") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedParentRecordX1 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX1Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();
        GenericRecord expectedParentRecordX2 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX2Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        Assert.assertEquals(expectedParentRecordX1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordX1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordX2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordX2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `b.y.d.k`
     *
     * Expected normalized results of the field `b.y.d.k`:
     *
     * [{"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]},
     * 
     * {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}]
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeSimpleFields_bydk() throws Exception {
        FlattenConfiguration configuration = getFlattenConfigurationWithTrimAndTrailing();
        configuration.setColumnToFlatten("b.y.d.k");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(2, outputs.size());

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

        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        Assert.assertEquals(expectedParentRecordK1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordK1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordK2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordK2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `b.y.d.k.t`
     *
     * Throw an exception: the element t does not exist
     *
     * @throws Exception
     */
    @Test(expected = TalendRuntimeException.class)
    public void testNormalizeSimpleFields_bydkt() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setColumnToFlatten("b.y.d.k.t");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        fnTester.processBundle(inputParentRecord);
    }

    /**
     * Input parent record: inputParentRecord_otherSeparator
     *
     * Normalize simple field `b.x` with separator `#`
     *
     * Expected normalized results of the field `b.x`:
     *
     * [{"a": "aaa", "b": {"x": "x1", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]},
     *
     * {"a": "aaa", "b": {"x": "x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}]
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeSimpleFields_otherSeparator() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setList(false);
        configuration.setFieldDelimiter(FlattenConfiguration.FieldDelimiterType.OTHER);
        configuration.setSpecificFieldDelimiter("#");
        configuration.setTrim(true);
        configuration.setDiscardTrailingEmptyStr(true);
        configuration.setColumnToFlatten("b.x");

        GenericRecord inputRecordXY_otherSeparator = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x1#x2") //
                .set("y", inputRecordDE) // listDE
                .build();

        GenericRecord inputParentRecord_otherSeparator = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", inputRecordXY_otherSeparator) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord_otherSeparator);
        Assert.assertEquals(2, outputs.size());

        GenericRecord expectedRecordX1Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x1") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedRecordX2Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x2") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedParentRecordX1 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX1Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();
        GenericRecord expectedParentRecordX2 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX2Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        Assert.assertEquals(expectedParentRecordX1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordX1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordX2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordX2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize array field: c.g`
     *
     * The schema of g must change from a list to a simple object. Expected normalized results of the field `c.g`:
     *
     * [{"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c":
     * {"f": "f", "g": {"h": "h1", "i": "i2"}}, "m": ["m1", "m2", "m3"]},
     * 
     * {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c":
     * {"f": "f", "g": {"h": "h2", "i": "i1"}}, "m": ["m1", "m2", "m3"]}]
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeArrayFields_cg() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setList(true);
        configuration.setColumnToFlatten("c.g");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(2, outputs.size());

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

        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        Assert.assertEquals(expectedParentRecordG1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordG1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordG2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordG2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize array field: `b.y.d.j`
     *
     * The schema of j must change from a list to a simple object Expected normalized results of the field `b.y.d.j`:
     *
     * [{ "a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l":"l1"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f": "f", "g":
     * [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]},
     * 
     * {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l":"l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f": "f", "g":
     * [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"] }]
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeArrayFields_bydj() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setList(true);
        configuration.setColumnToFlatten("b.y.d.j");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(2, outputs.size());

        Schema expectedSchemaL = SchemaBuilder.record("inputRowL") //
                .fields() //
                .name("l").type().optional().stringType() //
                .endRecord();

        Schema expectedSchemaJK = SchemaBuilder.record("inputRowJK") //
                .fields() //
                .name("j").type(expectedSchemaL).noDefault() //
                .name("k").type().optional().stringType() //
                .endRecord();

        Schema expectedSchemaDE = SchemaBuilder.record("inputRowDE") //
                .fields() //
                .name("d").type(expectedSchemaJK).noDefault() //
                .name("e").type().optional().stringType() //
                .endRecord();

        Schema expectedSchemaXY = SchemaBuilder.record("inputRowXY") //
                .fields() //
                .name("x").type().optional().stringType() //
                .name("y").type(expectedSchemaDE).noDefault() //
                .endRecord();

        Schema expectedParentSchema = SchemaBuilder.record("inputParentRow") //
                .fields() //
                .name("a").type().optional().stringType() //
                .name("b").type(expectedSchemaXY).noDefault() //
                .name("c").type(inputSchemaFG).noDefault() //
                .name("m").type(inputSchemaListM).noDefault() //
                .endRecord();

        GenericRecord expectedRecordJ1K = new GenericRecordBuilder(expectedSchemaJK) //
                .set("j", inputRecordL1) //
                .set("k", "k1;k2") //
                .build();
        GenericRecord expectedRecordDE1 = new GenericRecordBuilder(expectedSchemaDE) //
                .set("d", expectedRecordJ1K) //
                .set("e", "e") //
                .build();
        GenericRecord expectedRecordXY1 = new GenericRecordBuilder(expectedSchemaXY) //
                .set("x", "x1;x2") //
                .set("y", expectedRecordDE1) //
                .build();
        GenericRecord expectedParentRecordL1 = new GenericRecordBuilder(expectedParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordXY1) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        GenericRecord expectedRecordJ2K = new GenericRecordBuilder(expectedSchemaJK) //
                .set("j", inputRecordL2) //
                .set("k", "k1;k2") //
                .build();
        GenericRecord expectedRecordDE2 = new GenericRecordBuilder(expectedSchemaDE) //
                .set("d", expectedRecordJ2K) //
                .set("e", "e") //
                .build();
        GenericRecord expectedRecordXY2 = new GenericRecordBuilder(expectedSchemaXY) //
                .set("x", "x1;x2") //
                .set("y", expectedRecordDE2) //
                .build();
        GenericRecord expectedParentRecordL2 = new GenericRecordBuilder(expectedParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordXY2) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();
        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        Assert.assertEquals(expectedParentRecordL1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordL1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordL2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordL2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `m`
     *
     * The schema of m must change from a list to a simple object. Expected normalized results of the field `m`:
     *
     * [{"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c":
     * {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": "m1"},
     * 
     * {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c":
     * {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": "m2"},
     * 
     * {"a": "aaa", "b": {"x": "x1;x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c":
     * {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": "m3"}]
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeSimpleFields_m() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setList(false);
        configuration.setColumnToFlatten("m");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(3, outputs.size());

        Schema expectedParentSchema = SchemaBuilder.record("inputParentRow") //
                .fields() //
                .name("a").type().optional().stringType() //
                .name("b").type(inputSchemaXY).noDefault() //
                .name("c").type(inputSchemaFG).noDefault() //
                .name("m").type().stringType().noDefault() //
                .endRecord();

        GenericRecord expectedParentRecordM1 = new GenericRecordBuilder(expectedParentSchema) //
                .set("a", "aaa") //
                .set("b", inputRecordXY) //
                .set("c", inputRecordFG) //
                .set("m", "m1") //
                .build();

        GenericRecord expectedParentRecordM2 = new GenericRecordBuilder(expectedParentSchema) //
                .set("a", "aaa") //
                .set("b", inputRecordXY) //
                .set("c", inputRecordFG) //
                .set("m", "m2") //
                .build();

        GenericRecord expectedParentRecordM3 = new GenericRecordBuilder(expectedParentSchema) //
                .set("a", "aaa") //
                .set("b", inputRecordXY) //
                .set("c", inputRecordFG) //
                .set("m", "m3") //
                .build();

        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        GenericRecord outputRecord3 = (GenericRecord) outputs.get(2);
        Assert.assertEquals(expectedParentRecordM1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordM1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordM2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordM2.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordM3.toString(), outputRecord3.toString());
        Assert.assertEquals(expectedParentRecordM3.getSchema().toString(), outputRecord1.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize simple field: `b.y.d.j.l`
     *
     * Throw an exception: the element l is inside a loop.
     *
     * @throws Exception
     */
    @Test(expected = TalendRuntimeException.class)
    public void testNormalizeArrayFields_bydjl() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setColumnToFlatten("b.y.d.j.l");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        fnTester.processBundle(inputParentRecord);
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize complex field: `b`
     *
     * Expected: no change
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeComplexFields_b() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setColumnToFlatten("b");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(1, outputs.size());
        GenericRecord outputRecord = (GenericRecord) outputs.get(0);
        Assert.assertEquals(inputParentRecord.toString(), outputRecord.toString());
        Assert.assertEquals(inputParentRecord.getSchema().toString(), outputRecord.getSchema().toString());
    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize complex field: `b.y`
     *
     * Expected: no change
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeComplexFields_by() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setColumnToFlatten("b.y");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(1, outputs.size());
        GenericRecord outputRecord = (GenericRecord) outputs.get(0);
        Assert.assertEquals(inputParentRecord.toString(), outputRecord.toString());
        Assert.assertEquals(inputParentRecord.getSchema().toString(), outputRecord.getSchema().toString());

    }

    /**
     * Input parent record: {@link FlattenDoFnTest#inputParentRecord}
     *
     * Normalize complex field: `b.y.d`
     *
     * Expected: no change
     *
     * @throws Exception
     */
    @Test
    public void testNormalizeComplexFields_byd() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setColumnToFlatten("b.y.d");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(1, outputs.size());
        GenericRecord outputRecord = (GenericRecord) outputs.get(0);
        Assert.assertEquals(inputParentRecord.toString(), outputRecord.toString());
        Assert.assertEquals(inputParentRecord.getSchema().toString(), outputRecord.getSchema().toString());
    }

    /**
     * This test will normalize `b.x`. It will create 2 output. We are going to see if modifying the first output will
     * not have any impact on the second one.
     *
     * Normalize simple field: `b.x`
     *
     * Expected normalized results of the field `b.x`:
     *
     * [{"a": "aaa", "b": {"x": "x1", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]},
     * 
     * {"a": "aaa", "b": {"x": "x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}]
     *
     * After modification : [{"a": "MODIFIED_A", "b": {"x": "MODIFIED_X1", "y": {"d": {"j": [{"l": "MODIFIED_L1"}, {"l":
     * "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f": "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m":
     * ["m1", "m2", "m3", "MODIFIED_M1"]},
     * 
     * {"a": "aaa", "b": {"x": "x2", "y": {"d": {"j": [{"l": "l1"}, {"l": "l2"}], "k": "k1;k2"}, "e": "e"}}, "c": {"f":
     * "f", "g": [{"h": "h1", "i": "i2"}, {"h": "h2", "i": "i1"}]}, "m": ["m1", "m2", "m3"]}]
     *
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testVariableDuplication() throws Exception {
        FlattenConfiguration configuration = getFlattenConfigurationWithTrimAndTrailing();
        configuration.setList(false);
        configuration.setColumnToFlatten("b.x");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputParentRecord);
        Assert.assertEquals(2, outputs.size());

        GenericRecord expectedRecordX1Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x1") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedRecordX2Y = new GenericRecordBuilder(inputSchemaXY) //
                .set("x", "x2") //
                .set("y", inputRecordDE) //
                .build();
        GenericRecord expectedParentRecordX1 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX1Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();
        GenericRecord expectedParentRecordX2 = new GenericRecordBuilder(inputParentSchema) //
                .set("a", "aaa") //
                .set("b", expectedRecordX2Y) //
                .set("c", inputRecordFG) //
                .set("m", listInputRecordM) //
                .build();

        // test initial output
        GenericRecord outputRecord1 = (GenericRecord) outputs.get(0);
        GenericRecord outputRecord2 = (GenericRecord) outputs.get(1);
        Assert.assertEquals(expectedParentRecordX1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordX1.getSchema().toString(), outputRecord1.getSchema().toString());
        Assert.assertEquals(expectedParentRecordX2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordX2.getSchema().toString(), outputRecord2.getSchema().toString());

        // modify outputRecord1
        // Test a simple variable
        outputRecord1.put("a", "MODIFIED_A");
        // Test a hierarchical variable
        ((GenericRecord) outputRecord1.get("b")).put("x", "MODIFIED_X1");
        // Test a looped variable
        AbstractList<GenericRecord> j = (AbstractList<GenericRecord>) ((GenericRecord) ((GenericRecord) ((GenericRecord) outputRecord1
                .get("b")).get("y")).get("d")).get("j");
        j.get(0).put("l", "MODIFIED_L1");

        // Check outputRecord2
        Assert.assertNotEquals(expectedParentRecordX1.toString(), outputRecord1.toString());
        Assert.assertEquals(expectedParentRecordX2.toString(), outputRecord2.toString());
        Assert.assertEquals(expectedParentRecordX2.getSchema().toString(), outputRecord2.getSchema().toString());
    }

    /**
     * Normalize a field not present in the input record will throw TalendRuntimeException.
     *
     * @throws Exception
     */
    @Test(expected = TalendRuntimeException.class)
    public void testNormalizeNotFoundField() throws Exception {
        FlattenConfiguration configuration = getDefaultFlattenConfiguration();
        configuration.setColumnToFlatten("b.y.f");

        FlattenDoFn function = new FlattenDoFn().withConfiguration(configuration);

        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        fnTester.processBundle(inputParentRecord);
    }
}
