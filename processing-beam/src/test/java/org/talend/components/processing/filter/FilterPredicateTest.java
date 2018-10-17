package org.talend.components.processing.filter;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class FilterPredicateTest {

    @Test
    public void test_FilterSimple_utf8() {
        FilterConfiguration configuration = new FilterConfiguration();
        FilterConfiguration.Criteria filterProp = new FilterConfiguration.Criteria();
        filterProp.setColumnName("a");
        filterProp.setValue("aaa");
        configuration.getFilters().add(filterProp);

        Schema inputSimpleSchema = SchemaBuilder.record("inputRow") //
                .fields() //
                .name("a").type().optional().stringType() //
                .name("b").type().optional().stringType() //
                .name("c").type().optional().stringType() //
                .endRecord();

        GenericRecord inputSimpleRecord = new GenericRecordBuilder(inputSimpleSchema) //
                .set("a", new Utf8("aaa")) //
                .set("b", new Utf8("BBB")) //
                .set("c", new Utf8("Ccc")) //
                .build();

        FilterPredicate predicate = new FilterPredicate(configuration);
        assertThat(predicate.apply(inputSimpleRecord), is(Boolean.TRUE));

        predicate = new FilterPredicate.Negate(configuration);
        assertThat(predicate.apply(inputSimpleRecord), is(Boolean.FALSE));
    }
}