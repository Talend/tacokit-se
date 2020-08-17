package org.talend.components.common.stream.output.line;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.time.ZoneId;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RecordSerializerLineHelperTest {

    private RecordBuilderFactory recordBuilderFactory;

    @BeforeEach
    public void before() {
        recordBuilderFactory = new RecordBuilderFactoryImpl("test");
    }

    @Test
    public void valuesFrom_simple() {
        final Record record = buildRecords_simple();
        final List<String> strings = RecordSerializerLineHelper.valuesFrom(record);

        assertEquals("Smith", strings.get(0));
        assertEquals("35", strings.get(1));
        assertEquals("true", strings.get(2));
        assertEquals("2020-08-17T10:10:10.010Z[UTC]", strings.get(3));
    }

    private Record buildRecords_simple() {
        GregorianCalendar gc = new GregorianCalendar();
        gc.set(GregorianCalendar.YEAR, 2020);
        gc.set(GregorianCalendar.MONTH, 7);
        gc.set(GregorianCalendar.DAY_OF_MONTH, 17);
        gc.set(GregorianCalendar.HOUR_OF_DAY, 10);
        gc.set(GregorianCalendar.MINUTE, 10);
        gc.set(GregorianCalendar.SECOND, 10);
        gc.set(GregorianCalendar.MILLISECOND, 10);
        gc.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
        final Date date = gc.getTime();

        final Record rec1 = recordBuilderFactory.newRecordBuilder()
                .withString(createEntry("name", Schema.Type.STRING, false), "Smith")
                .withInt(createEntry("age", Schema.Type.INT, false), 35)
                .withBoolean(createEntry("registered", Schema.Type.BOOLEAN, false), true)
                .withDateTime(createEntry("last_save", Schema.Type.DATETIME, false), date)
                .build();
        return rec1;
    }

    @Test
    public void valuesFrom_withNull() {
        final Record record = buildRecords_withNull();
        final List<String> strings = RecordSerializerLineHelper.valuesFrom(record);

        assertEquals("Smith", strings.get(0));
        assertEquals("35", strings.get(1));
        assertEquals("true", strings.get(2));
        assertNull(strings.get(3));
    }

    private Record buildRecords_withNull() {
        GregorianCalendar gc = new GregorianCalendar();
        gc.set(GregorianCalendar.YEAR, 2020);
        gc.set(GregorianCalendar.MONTH, 7);
        gc.set(GregorianCalendar.DAY_OF_MONTH, 17);
        gc.set(GregorianCalendar.HOUR_OF_DAY, 10);
        gc.set(GregorianCalendar.MINUTE, 10);
        gc.set(GregorianCalendar.SECOND, 10);
        gc.set(GregorianCalendar.MILLISECOND, 10);
        gc.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
        final Date date = gc.getTime();

        final Record rec1 = recordBuilderFactory.newRecordBuilder()
                .withString(createEntry("name", Schema.Type.STRING, false), "Smith")
                .withInt(createEntry("age", Schema.Type.INT, false), 35)
                .withBoolean(createEntry("registered", Schema.Type.BOOLEAN, false), true)
                .withDateTime(createEntry("last_save", Schema.Type.DATETIME, true), (Date) null)
                .build();
        return rec1;
    }

    private Schema.Entry createEntry(final String name, final Schema.Type type, final boolean nullable) {
        return recordBuilderFactory.newEntryBuilder().withName(name).withRawName(name).withType(type).withNullable(nullable).build();
    }

}