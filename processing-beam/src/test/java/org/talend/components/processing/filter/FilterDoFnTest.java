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
package org.talend.components.processing.filter;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Ignore;
import org.junit.Test;
import org.talend.daikon.exception.TalendRuntimeException;

public class FilterDoFnTest {

    private final Schema inputSimpleSchema = SchemaBuilder.record("inputRow") //
            .fields() //
            .name("a").type().optional().stringType() //
            .name("b").type().optional().stringType() //
            .name("c").type().optional().stringType() //
            .endRecord();

    private final GenericRecord inputSimpleRecord = new GenericRecordBuilder(inputSimpleSchema) //
            .set("a", "aaa") //
            .set("b", "BBB") //
            .set("c", "Ccc") //
            .build();

    private final Schema inputNumericSchema = SchemaBuilder.record("inputRow") //
            .fields() //
            .name("a").type().optional().intType() //
            .name("b").type().optional().intType() //
            .name("c").type().optional().intType() //
            .endRecord();

    private final GenericRecord inputNumericRecord = new GenericRecordBuilder(inputSimpleSchema) //
            .set("a", 10) //
            .set("b", -100) //
            .set("c", 1000) //
            .build();

    private final GenericRecord input_10_100_1000_Record = new GenericRecordBuilder(inputSimpleSchema) //
            .set("a", 10) //
            .set("b", 100) //
            .set("c", 1000) //
            .build();

    private final GenericRecord input_20_200_2000_Record = new GenericRecordBuilder(inputSimpleSchema) //
            .set("a", 20) //
            .set("b", 200) //
            .set("c", 2000) //
            .build();

    private final GenericRecord input_30_300_3000_Record = new GenericRecordBuilder(inputSimpleSchema) //
            .set("a", 30) //
            .set("b", 300) //
            .set("c", 3000) //
            .build();

    /**
     * Creates and returns {@link FilterConfiguration} by adding a new criteria.
     * <p>
     * TODO: most of the tests below use the default property with empty columnName and value, PLUS the criteria under test.
     * This method can ensure that only the requested criteria are in the configuration.
     *
     * @param frp The configuration to modify by adding a new {@link FilterConfiguration.Criteria} instance. If this is null, a
     * new instance is created.
     * @param columnName If non-null, sets the columnName of the criteria.
     * @param function If non-null, sets the function of the criteria.
     * @param operator If non-null, sets the operator of the criteria.
     * @param value If non-null, sets the value of the criteria.
     * @return a configuration with the specified criteria added.
     */
    protected static FilterConfiguration addCriteria(FilterConfiguration frp, String columnName,
            ConditionsRowConstant.Function function, ConditionsRowConstant.Operator operator, String value) {
        // Create a new configuration if one wasn't passed in.
        if (frp == null) {
            frp = new FilterConfiguration();
            // Remove the default critera.
            frp.getFilters().clear();
        }

        // Create and add a new criteria with the requested configuration.
        FilterConfiguration.Criteria criteria = new FilterConfiguration.Criteria();
        frp.getFilters().add(criteria);

        if (columnName != null)
            criteria.setColumnName(columnName);
        if (function != null)
            criteria.setFunction(function);
        if (operator != null)
            criteria.setOperator(operator);
        if (value != null)
            criteria.setValue(value);

        return frp;
    }

    private void checkSimpleInputNoOutput(DoFnTester<Object, IndexedRecord> fnTester) throws Exception {
        List<IndexedRecord> outputs = fnTester.processBundle(inputSimpleRecord);
        assertEquals(0, outputs.size());
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);
        assertEquals(0, rejects.size());
    }

    private void checkSimpleInputValidOutput(DoFnTester<IndexedRecord, IndexedRecord> fnTester) throws Exception {
        List<IndexedRecord> outputs = fnTester.processBundle(inputSimpleRecord);
        assertEquals(1, outputs.size());
        assertEquals("aaa", outputs.get(0).get(0));
        assertEquals("BBB", outputs.get(0).get(1));
        assertEquals("Ccc", outputs.get(0).get(2));
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);
        assertEquals(0, rejects.size());
    }

    private void checkSimpleInputInvalidOutput(DoFnTester<IndexedRecord, IndexedRecord> fnTester) throws Exception {
        List<IndexedRecord> outputs = fnTester.processBundle(inputSimpleRecord);
        assertEquals(0, outputs.size());
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);
        assertEquals(1, rejects.size());
        assertEquals("aaa", rejects.get(0).get(0));
        assertEquals("BBB", rejects.get(0).get(1));
        assertEquals("Ccc", rejects.get(0).get(2));
    }

    private void checkNumericInputNoOutput(DoFnTester<Object, IndexedRecord> fnTester) throws Exception {
        List<IndexedRecord> outputs = fnTester.processBundle(inputNumericRecord);
        assertEquals(0, outputs.size());
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);
        assertEquals(0, rejects.size());
    }

    private void checkNumericInputValidOutput(DoFnTester<IndexedRecord, IndexedRecord> fnTester) throws Exception {
        List<IndexedRecord> outputs = fnTester.processBundle(inputNumericRecord);
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);
        assertEquals(1, outputs.size());
        assertEquals(10, outputs.get(0).get(0));
        assertEquals(-100, outputs.get(0).get(1));
        assertEquals(1000, outputs.get(0).get(2));
        assertEquals(0, rejects.size());
    }

    private void checkNumericInputInvalidOutput(DoFnTester<IndexedRecord, IndexedRecord> fnTester) throws Exception {
        List<IndexedRecord> outputs = fnTester.processBundle(inputNumericRecord);
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);
        assertEquals(0, outputs.size());
        assertEquals(1, rejects.size());
        assertEquals(10, rejects.get(0).get(0));
        assertEquals(-100, rejects.get(0).get(1));
        assertEquals(1000, rejects.get(0).get(2));
    }

    private void runSimpleTestValidSession(FilterConfiguration configuration) throws Exception {
        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        checkSimpleInputValidOutput(fnTester);
    }

    private void runSimpleTestInvalidSession(FilterConfiguration configuration) throws Exception {
        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        checkSimpleInputInvalidOutput(fnTester);
    }

    private void runNumericTestValidSession(FilterConfiguration configuration) throws Exception {
        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        checkNumericInputValidOutput(fnTester);
    }

    private void runNumericTestInvalidSession(FilterConfiguration configuration) throws Exception {
        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        checkNumericInputInvalidOutput(fnTester);
    }

    /**
     * Check that any filter row that contains an "empty" or "default" criteria passes all records.
     */
    @Test
    public void test_FilterWithDefaultValue() throws Exception {
        // Create a filter row with exactly one criteria that hasn't been filled by the user.
        FilterConfiguration configuration = addCriteria(null, null, null, null, null);
        assertThat(configuration.getFilters(), hasSize(1));

        FilterConfiguration.Criteria criteria = configuration.getFilters().iterator().next();
        assertThat(criteria.getColumnName(), is(""));
        assertThat(criteria.getFunction().getValue(), is("EMPTY"));
        assertThat(criteria.getOperator().getValue(), is("=="));
        assertThat(criteria.getValue(), is(""));

        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        checkSimpleInputValidOutput(fnTester);
    }

    @Test
    public void test_FilterSimple_Valid() throws Exception {
        FilterConfiguration configuration = addCriteria(null, "a", ConditionsRowConstant.Function.EMPTY,
                ConditionsRowConstant.Operator.EQUAL, "aaa");
        assertThat(configuration.getFilters(), hasSize(1));

        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterSimple_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();
        filterProp.setColumnName("a");
        filterProp.setValue("c");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Ignore("TODO: handle unknown or invalid columns.")
    @Test(expected = TalendRuntimeException.class)
    public void test_invalidColumnName() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();
        filterProp.setColumnName("INVALID");
        filterProp.setValue("aa");
        configuration.getFilters().add(filterProp);
        // Will throw an exception
        runSimpleTestInvalidSession(configuration);
    }

    /**
     * Test every function possible
     */
    @Test
    public void test_FilterAbsolute_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();
        filterProp.setColumnName("b");
        filterProp.setFunction(ConditionsRowConstant.Function.ABS_VALUE);
        filterProp.setValue("100");
        configuration.getFilters().add(filterProp);
        runNumericTestValidSession(configuration);
    }

    @Test
    public void test_FilterAbsolute_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("b");
        filterProp.setFunction(ConditionsRowConstant.Function.ABS_VALUE);
        filterProp.setValue("-100");
        configuration.getFilters().add(filterProp);
        runNumericTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterLowerCase_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("b");
        filterProp.setFunction(ConditionsRowConstant.Function.LOWER_CASE);
        filterProp.setValue("bbb");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterLowerCase_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("b");
        filterProp.setFunction(ConditionsRowConstant.Function.LOWER_CASE);
        filterProp.setValue("BBB");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterUpperCase_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setFunction(ConditionsRowConstant.Function.UPPER_CASE);
        filterProp.setValue("AAA");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterUpperCase_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setFunction(ConditionsRowConstant.Function.UPPER_CASE);
        filterProp.setValue("aaa");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterFirstCharLowerCase_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("b");
        filterProp.setFunction(ConditionsRowConstant.Function.FIRST_CHARACTER_LOWER_CASE);
        filterProp.setValue("b");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterFirstCharLowerCase_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("b");
        filterProp.setFunction(ConditionsRowConstant.Function.FIRST_CHARACTER_LOWER_CASE);
        filterProp.setValue("BBB");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterFirstCharUpperCase_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setFunction(ConditionsRowConstant.Function.FIRST_CHARACTER_UPPER_CASE);
        filterProp.setValue("A");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterFirstCharUpperCase_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setFunction(ConditionsRowConstant.Function.FIRST_CHARACTER_UPPER_CASE);
        filterProp.setValue("a");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterLength_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setFunction(ConditionsRowConstant.Function.LENGTH);
        filterProp.setValue("3");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterLength_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setFunction(ConditionsRowConstant.Function.LENGTH);
        filterProp.setValue("4");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    /**
     * Test every operation possible
     */

    @Test
    public void test_FilterNotEquals_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.NOT_EQUAL);
        filterProp.setValue("aaaa");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterNotEquals_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.NOT_EQUAL);
        filterProp.setValue("aaa");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterLowerThan_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.LOWER);
        filterProp.setValue("1001");
        configuration.getFilters().add(filterProp);
        runNumericTestValidSession(configuration);
    }

    @Test
    public void test_FilterLowerThan_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.LOWER);
        filterProp.setValue("1000");
        configuration.getFilters().add(filterProp);
        runNumericTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterGreaterThan_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.GREATER);
        filterProp.setValue("999");
        configuration.getFilters().add(filterProp);
        runNumericTestValidSession(configuration);
    }

    @Test
    public void test_FilterGeaterThan_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.GREATER);
        filterProp.setValue("1000");
        configuration.getFilters().add(filterProp);
        runNumericTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterLowerOrEqualsThan_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.LOWER_OR_EQUAL);
        filterProp.setValue("1000");
        configuration.getFilters().add(filterProp);
        runNumericTestValidSession(configuration);
    }

    @Test
    public void test_FilterLowerOrEqualsThan_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.LOWER_OR_EQUAL);
        filterProp.setValue("999");
        configuration.getFilters().add(filterProp);
        runNumericTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterGreaterOrEqualsThan_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.GREATER_OR_EQUAL);
        filterProp.setValue("1000");
        configuration.getFilters().add(filterProp);
        runNumericTestValidSession(configuration);
    }

    @Test
    public void test_FilterGeaterOrEqualsThan_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("c");
        filterProp.setOperator(ConditionsRowConstant.Operator.GREATER_OR_EQUAL);
        filterProp.setValue("1001");
        configuration.getFilters().add(filterProp);
        runNumericTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterBetween() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterGreater = new FilterConfiguration.Criteria();
        filterGreater.setColumnName("a");
        filterGreater.setOperator(ConditionsRowConstant.Operator.GREATER);
        filterGreater.setValue("10");
        configuration.getFilters().add(filterGreater);
        FilterConfiguration.Criteria filterLess = new FilterConfiguration.Criteria();
        filterLess.setColumnName("a");
        filterLess.setOperator(ConditionsRowConstant.Operator.LOWER);
        filterLess.setValue("30");
        configuration.getFilters().add(filterLess);

        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);

        List<IndexedRecord> outputs = fnTester.processBundle(input_30_300_3000_Record, input_10_100_1000_Record,
                input_20_200_2000_Record);
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);

        assertEquals(1, outputs.size());
        assertEquals(2, rejects.size());

    }

    @Test
    public void test_FilterMatch_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.MATCH);
        filterProp.setValue("^aaa$");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterMatch_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.MATCH);
        filterProp.setValue("^aaaa$");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterNotMatch_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.NOT_MATCH);
        filterProp.setValue("^aaaa$");
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterNotMatch_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.NOT_MATCH);
        filterProp.setValue("^aaa$");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterContains_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.CONTAINS);
        filterProp.setValue("aa");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterContains_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.CONTAINS);
        filterProp.setValue("aaaa");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterNotContains_Valid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.NOT_CONTAINS);
        filterProp.setValue("aaaa");
        configuration.getFilters().add(filterProp);
        runSimpleTestValidSession(configuration);
    }

    @Test
    public void test_FilterNotContains_Invalid() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();

        filterProp.setColumnName("a");
        filterProp.setOperator(ConditionsRowConstant.Operator.NOT_CONTAINS);
        filterProp.setValue("aa");
        configuration.getFilters().add(filterProp);
        runSimpleTestInvalidSession(configuration);
    }

    @Test
    public void test_FilterLogicalOpAny() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        configuration.setLogicalOpType(LogicalOpType.ANY);
        FilterConfiguration.Criteria condition1 = new FilterConfiguration.Criteria();
        condition1.setColumnName("a");
        condition1.setOperator(ConditionsRowConstant.Operator.EQUAL);
        condition1.setValue("10");
        configuration.getFilters().add(condition1);
        FilterConfiguration.Criteria condition2 = new FilterConfiguration.Criteria();
        condition2.setColumnName("b");
        condition2.setOperator(ConditionsRowConstant.Operator.EQUAL);
        condition2.setValue("300");
        configuration.getFilters().add(condition2);

        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);

        List<IndexedRecord> outputs = fnTester.processBundle(input_30_300_3000_Record, input_10_100_1000_Record,
                input_20_200_2000_Record);
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);

        assertEquals(2, outputs.size());
        assertEquals(30, outputs.get(0).get(0));
        assertEquals(10, outputs.get(1).get(0));
        assertEquals(1, rejects.size());
        assertEquals(20, rejects.get(0).get(0));
    }

    @Test
    public void test_FilterLogicalOpNone() throws Exception {
        FilterConfiguration configuration = new FilterConfiguration();
        configuration.setLogicalOpType(LogicalOpType.NONE);
        FilterConfiguration.Criteria condition1 = new FilterConfiguration.Criteria();
        condition1.setColumnName("a");
        condition1.setOperator(ConditionsRowConstant.Operator.EQUAL);
        condition1.setValue("10");
        configuration.getFilters().add(condition1);
        FilterConfiguration.Criteria condition2 = new FilterConfiguration.Criteria();
        condition2.setColumnName("b");
        condition2.setOperator(ConditionsRowConstant.Operator.EQUAL);
        condition2.setValue("100");
        configuration.getFilters().add(condition2);
        FilterConfiguration.Criteria condition3 = new FilterConfiguration.Criteria();
        condition3.setColumnName("a");
        condition3.setOperator(ConditionsRowConstant.Operator.EQUAL);
        condition3.setValue("20");
        configuration.getFilters().add(condition3);

        FilterDoFn function = new FilterDoFn(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);

        List<IndexedRecord> outputs = fnTester.processBundle(input_30_300_3000_Record, input_10_100_1000_Record,
                input_20_200_2000_Record);
        List<IndexedRecord> rejects = fnTester.peekOutputElements(Filter.rejectOutput);

        assertEquals(1, outputs.size());
        assertEquals(2, rejects.size());

    }
}
