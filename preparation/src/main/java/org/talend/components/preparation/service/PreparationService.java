package org.talend.components.preparation.service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collector;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Service
public class PreparationService {

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    public Collector<Schema.Entry, Record.Builder, Record> toRecord(final Schema schema, final Record fallbackRecord,
            final BiFunction<Schema.Entry, Record.Builder, Boolean> customHandler,
            final BiConsumer<Record.Builder, Boolean> beforeFinish) {
        final AtomicBoolean customHandlerCalled = new AtomicBoolean();
        return Collector.of(() -> recordBuilderFactory.newRecordBuilder(schema), (builder, entry) -> {
            if (!customHandler.apply(entry, builder)) {
                forwardEntry(fallbackRecord, builder, entry.getName(), entry);
            } else {
                customHandlerCalled.set(true);
            }
        }, (b1, b2) -> {
            throw new IllegalStateException("merge unsupported");
        }, builder -> {
            beforeFinish.accept(builder, customHandlerCalled.get());
            return builder.build();
        });
    }

    public void forwardEntry(final Record source, final Record.Builder builder, final String sourceColumn,
            final Schema.Entry entry) {
        switch (entry.getType()) {
        case INT:
            source.getOptionalInt(sourceColumn).ifPresent(v -> builder.withInt(entry, v));
            break;
        case LONG:
            source.getOptionalLong(sourceColumn).ifPresent(v -> builder.withLong(entry, v));
            break;
        case FLOAT:
            source.getOptionalFloat(sourceColumn).ifPresent(v -> builder.withFloat(entry, (float) v));
            break;
        case DOUBLE:
            source.getOptionalDouble(sourceColumn).ifPresent(v -> builder.withDouble(entry, v));
            break;
        case BOOLEAN:
            source.getOptionalBoolean(sourceColumn).ifPresent(v -> builder.withBoolean(entry, v));
            break;
        case STRING:
            source.getOptionalString(sourceColumn).ifPresent(v -> builder.withString(entry, v));
            break;
        case DATETIME:
            source.getOptionalDateTime(sourceColumn).ifPresent(v -> builder.withDateTime(entry, v));
            break;
        case BYTES:
            source.getOptionalBytes(sourceColumn).ifPresent(v -> builder.withBytes(entry, v));
            break;
        case RECORD:
            source.getOptionalRecord(sourceColumn).ifPresent(v -> builder.withRecord(entry, v));
            break;
        case ARRAY:
            source.getOptionalArray(Object.class, sourceColumn).ifPresent(v -> builder.withArray(entry, v));
            break;
        default:
            throw new IllegalStateException("Unsupported entry type: " + entry);
        }
    }

}