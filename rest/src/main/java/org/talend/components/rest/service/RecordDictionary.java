package org.talend.components.rest.service;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointer;
import org.talend.sdk.component.api.record.RecordPointerFactory;

import java.util.Collection;
import java.util.function.UnaryOperator;

import static java.util.Optional.ofNullable;

public class RecordDictionary implements UnaryOperator<String> {

    private final Record record;

    private final RecordPointerFactory recordPointerFactory;

    public RecordDictionary(Record record, RecordPointerFactory recordPointerFactory) {
        this.record = record;
        this.recordPointerFactory = recordPointerFactory;
    }

    public String apply(String key) {
        final RecordPointer rp = recordPointerFactory.apply(key);
        try {
            final Object value = rp.getValue(record, Object.class);

            return ofNullable(value)
                    .filter(v -> !(Record.class.isInstance(v) || Collection.class.isInstance(v)))
                    .map(String::valueOf)
                    .orElse(null); // If other than null, then ':-' default syntax in place holder
                                         // is not taken into account
        }
        catch (IllegalArgumentException ex) {
            return null;
        }
    }


}
