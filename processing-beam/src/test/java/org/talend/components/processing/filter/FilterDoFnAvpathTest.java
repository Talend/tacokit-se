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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.talend.components.processing.SampleAvpathSchemas.SyntheticDatasets.copyAndReplaceSubrecordArray;
import static org.talend.components.processing.SampleAvpathSchemas.SyntheticDatasets.getSubrecords;
import static org.talend.components.processing.filter.FilterDoFnTest.addCriteria;

import java.util.List;
import java.util.Random;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.talend.components.processing.ProcessingErrorCode;
import org.talend.components.processing.SampleAvpathSchemas;
import org.talend.daikon.exception.TalendRuntimeException;

/**
 * Tests avpath expressions.
 *
 * @see <a href="https://jira.talendforge.org/browse/TFD-2119">TFD-2119</a>.
 */
public class FilterDoFnAvpathTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final IndexedRecord[] inputA = SampleAvpathSchemas.SyntheticDatasets.getRandomRecords(1000, new Random(0),
            SampleAvpathSchemas.SyntheticDatasets.RECORD_A);

    private final IndexedRecord[] inputB = SampleAvpathSchemas.SyntheticDatasets.getRandomRecords(1000, new Random(0),
            SampleAvpathSchemas.SyntheticDatasets.RECORD_B);

    // private final IndexedRecord[] inputC = SampleAvpathSchemas.SyntheticDatasets.getRandomRecords(1000, new Random(0),
    // SampleAvpathSchemas.SyntheticDatasets.RECORD_C);

    @Test
    public void testBasicHierarchical() throws Exception {
        FilterConfiguration properties = addCriteria(null, ".automobiles{.maker === \"Toyota\"}.year", null,
                ConditionsRowConstant.Operator.GREATER, "2015");

        IndexedRecord input = SampleAvpathSchemas.Vehicles.getDefaultVehicleCollection();
        // All of the Toyota automobiles were made after 2015, so record accepted.
        assertThat(new FilterPredicate(properties).apply(input), is(Boolean.TRUE));

        // Not all of the Honda automobiles were made after 2009, so record rejected.

        FilterConfiguration.Criteria criteria = properties.getFilters().get(0);
        criteria.setColumnName(".automobiles{.maker === \"Honda\"}.year");
        criteria.setOperator(ConditionsRowConstant.Operator.GREATER);
        criteria.setValue("2009");
        assertThat(new FilterPredicate(properties).apply(input), is(Boolean.FALSE));
    }

    @Test
    public void testHierarchical_TFD2119_ERR1_UnknownColumn() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".unknown", //
                        null, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "unknown") //

                ));

        List<IndexedRecord> output = fnTester.processBundle(inputA);

        // None of the records can possibly match.
        // TODO(TFD-2194): This should throw an exception if possible.
        // Until that is the case, the output should at least be empty.
        assertThat(output, empty());
    }

    @Test
    public void testHierarchical_TFD2119_ERR2_SyntaxError() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        "asdf&*{.\\t", //
                        null, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "unknown") //
                ));

        // None of the records can possibly match, and a syntax error message is thrown.
        thrown.expect(TalendRuntimeException.class);
        thrown.expect(hasProperty("code", is(ProcessingErrorCode.AVPATH_SYNTAX_ERROR)));
        thrown.expectMessage("The avpath query '.asdf&*{.\\t' is invalid.");
        List<IndexedRecord> output = fnTester.processBundle(inputA);
    }

    @Test
    public void testHierarchical_TFD2119_ERR3_ArrayOutOfBounds() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1[99].id", //
                        null, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "1") //
                ));

        // Looks like this is not an exception -- it considers .b1[99] to be the last record.
        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            List<IndexedRecord> subrecords = getSubrecords(main);
            assertThat(main.toString(), subrecords.get(subrecords.size() - 1).get(0), is((Object) 1));
        }
        assertThat(output, hasSize(114));
    }

    @Test
    public void testHierarchical_TFD2119_ERR4_NullArray() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1[0].id", //
                        null, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "1") //
                ));

        // Looks like this is not an exception -- it just doesn't match.
        IndexedRecord[] input = copyAndReplaceSubrecordArray(inputB, 10, true);
        List<IndexedRecord> output = fnTester.processBundle(input);
        for (IndexedRecord main : output) {
            List<IndexedRecord> subrecords = getSubrecords(main);
            assertThat(main.toString(), subrecords.get(0).get(0), is((Object) 1));
        }
        assertThat(output, hasSize(102));
    }

    @Test
    public void testHierarchical_TFD2119_A1_TopLevel() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".id", //
                        null, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "1") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputA);
        for (IndexedRecord main : output) {
            assertThat(main.toString(), main.get(0), is((Object) 1));
        }
        assertThat(output, hasSize(103));
    }

    @Test
    public void testHierarchical_TFD2119_A2_Subrecord() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".a1.id", //
                        null, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "1") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputA);
        for (IndexedRecord main : output) {
            List<IndexedRecord> subrecords = getSubrecords(main);
            assertThat(main.toString(), subrecords.get(0).get(0), is((Object) 1));
        }
        assertThat(output, hasSize(98));
    }

    @Test
    public void testHierarchical_TFD2119_A3_Subsubrecord() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".a1.a2.id", //
                        null, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "1") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputA);
        for (IndexedRecord main : output) {
            List<IndexedRecord> subrecords = getSubrecords(main);
            List<IndexedRecord> subsubrecords = getSubrecords(subrecords.get(0));
            assertThat(main.toString(), subsubrecords.get(0).get(0), is((Object) 1));
        }
        assertThat(output, hasSize(117));
    }

    @Test
    public void testHierarchical_TFD2119_B1_AtLeastOneSubRecordHasValueGt10() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1{.value > 10}", //
                        ConditionsRowConstant.Function.COUNT, //
                        ConditionsRowConstant.Operator.GREATER, //
                        "0") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            boolean atLeastOne = false;
            for (IndexedRecord subrecord : getSubrecords(main)) {
                if ((double) subrecord.get(2) > 10)
                    atLeastOne = true;
            }
            assertThat(main.toString(), atLeastOne, is(true));
        }
        assertThat(output, hasSize(274));
    }

    @Test
    public void testHierarchical_TFD2119_B2_AllSubRecordsHaveValueGt10() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1{.value <= 10}", //
                        ConditionsRowConstant.Function.COUNT, //
                        ConditionsRowConstant.Operator.EQUAL, //
                        "0") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            for (IndexedRecord subrecord : getSubrecords(main)) {
                assertThat(main.toString(), (double) subrecord.get(2), greaterThan(10d));
            }
        }
        assertThat(output, hasSize(58));
    }

    @Test
    public void testHierarchical_TFD2119_B3_FirstRecordValueGt10() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1[0].value", //
                        null, //
                        ConditionsRowConstant.Operator.GREATER, //
                        "10") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            assertThat(main.toString(), (double) getSubrecords(main).get(0).get(2), greaterThan(10d));
        }
        assertThat(output, hasSize(155));
    }

    @Test
    public void testHierarchical_TFD2119_B4_LastRecordValueGt10() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1[-1].value", //
                        null, //
                        ConditionsRowConstant.Operator.GREATER, //
                        "10") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            List<IndexedRecord> subrecords = getSubrecords(main);
            assertThat(main.toString(), (double) subrecords.get(subrecords.size() - 1).get(2), greaterThan(10d));
        }
        assertThat(output, hasSize(145));
    }

    @Ignore("Parenthesis are not correctly implemented in avpath.")
    @Test
    public void testHierarchical_TFD2119_B5_AtLeast1SubRecordsWithId1Or2HasValueGt10() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1{(.id == 1) || (.id == 2)) && .value > 10}", //
                        ConditionsRowConstant.Function.COUNT, //
                        ConditionsRowConstant.Operator.GREATER, //
                        "0") //
                ));

        // TODO: for the moment, this just throws an exception.
        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            boolean atLeastOne = false;
            for (IndexedRecord subrecord : getSubrecords(main)) {
                int id = (int) subrecord.get(0);
                if ((double) subrecord.get(2) > 10 && (id == 1 || id == 2))
                    atLeastOne = true;
            }
            assertThat(main.toString(), atLeastOne, is(true));
        }
        assertThat(output, hasSize(57));
    }

    @Test
    public void testHierarchical_TFD2119_B5_AtLeast1SubRecordsWithId1Or2HasValueGt10_Alternative() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1{.id == 1 && .value > 10 || .id == 2 && .value > 10}", //
                        ConditionsRowConstant.Function.COUNT, //
                        ConditionsRowConstant.Operator.GREATER, //
                        "0") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputB);
        for (IndexedRecord main : output) {
            boolean atLeastOne = false;
            for (IndexedRecord subrecord : getSubrecords(main)) {
                int id = (int) subrecord.get(0);
                if ((double) subrecord.get(2) > 10 && (id == 1 || id == 2))
                    atLeastOne = true;
            }
            assertThat(main.toString(), atLeastOne, is(true));
        }
        assertThat(output, hasSize(57));
    }

    @Test
    public void testHierarchical_TFD2119_B6_AllSubRecordsWithId1Or2HasValueGt10() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1{.id == 1 || .id == 2}.value", //
                        null, //
                        ConditionsRowConstant.Operator.GREATER, //
                        "10") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputB);

        for (IndexedRecord main : output) {
            boolean atLeastOne = false;
            for (IndexedRecord subrecord : getSubrecords(main)) {
                int id = (int) subrecord.get(0);
                if (id == 1 || id == 2) {
                    atLeastOne = true;
                    assertThat(main.toString(), (double) subrecord.get(2), greaterThan(10d));
                }
            }
            assertThat(main.toString(), atLeastOne, is(true));
        }
        assertThat(output, hasSize(42));
    }

    @Test
    public void testHierarchical_TFD2119_B7_HasAtLeastOneSubrecordWithSubSubRecordValueGt10() throws Exception {
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of( //
                new FilterDoFn(addCriteria(null, //
                        ".b1{.b2.value > 10}", //
                        ConditionsRowConstant.Function.COUNT, //
                        ConditionsRowConstant.Operator.GREATER, //
                        "0") //
                ));

        List<IndexedRecord> output = fnTester.processBundle(inputB);

        for (IndexedRecord main : output) {
            boolean atLeastOne = false;
            for (IndexedRecord subrecord : getSubrecords(main)) {
                for (IndexedRecord subsubrecord : getSubrecords(subrecord)) {
                    if ((double) subsubrecord.get(2) > 10)
                        atLeastOne = true;
                }
            }
            assertThat(main.toString(), atLeastOne, is(true));
        }
        assertThat(output, hasSize(311));
    }

}
