package org.talend.components.preparation.bool;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.components.preparation.configuration.NewColumn;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.output.Processor;

@WithComponents("org.talend.components.preparation")
class NegateTest {

    @Injected
    private ComponentsHandler handler;

    @Service
    private RecordBuilderFactory records;

    @Test
    void simpleNegate() {
        final Negate.Configuration configuration = new Negate.Configuration();
        configuration.setColumn("item");

        final Processor processor = handler.createProcessor(Negate.class, configuration);

        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(records.newRecordBuilder().withBoolean("item", true).withString("name", "first").build(),
                        records.newRecordBuilder().withBoolean("item", false).withString("name", "second").build()));

        final List<Record> output = handler.collect(processor, joinInputFactory).get(Record.class, "__default__");
        assertEquals(asList("false/first", "true/second"),
                output.stream().map(it -> it.getBoolean("item") + "/" + it.getString("name")).collect(toList()));
    }

    @Test
    void defaultValue() {
        final Negate.Configuration configuration = new Negate.Configuration();
        configuration.setColumn("item");
        configuration.setDefaultValue(true);

        final Processor processor = handler.createProcessor(Negate.class, configuration);

        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(records.newRecordBuilder().withString("name", "first").build(),
                        records.newRecordBuilder().withBoolean("item", false).withString("name", "second").build()));

        final List<Record> output = handler.collect(processor, joinInputFactory).get(Record.class, "__default__");
        assertEquals(asList("true/first", "true/second"),
                output.stream().map(it -> it.getBoolean("item") + "/" + it.getString("name")).collect(toList()));
    }

    @Test
    void newColumn() {
        final Negate.Configuration configuration = new Negate.Configuration();
        configuration.setColumn("item");
        configuration.setNewColumn(new NewColumn());
        configuration.getNewColumn().setCreate(true);
        configuration.getNewColumn().setName("negated");

        final Processor processor = handler.createProcessor(Negate.class, configuration);

        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(records.newRecordBuilder().withBoolean("item", true).withString("name", "first").build(),
                        records.newRecordBuilder().withBoolean("item", false).withString("name", "second").build()));

        final List<Record> output = handler.collect(processor, joinInputFactory).get(Record.class, "__default__");
        assertEquals(asList("true/first/false", "false/second/true"),
                output.stream().map(it -> it.getBoolean("item") + "/" + it.getString("name") + "/" + it.getBoolean("negated"))
                        .collect(toList()));
    }
}