package org.talend.components.jdbc.components;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;

@Version
@Icon(Icon.IconType.SAMPLE)
@PartitionMapper(name = "RowGenerator", family = "jdbcTest")
public class RowGeneratorMapper implements Serializable {

    private final RowGeneratorSource.Config config;

    private RecordBuilderFactory recordBuilderFactory;

    private final long recordSize;

    public RowGeneratorMapper(@Option("config") final RowGeneratorSource.Config config,
            final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
        this.recordSize = recordBuilderFactory.newRecordBuilder().withString("t_string", "data0").withBoolean("t_boolean", true)
                .withLong("t_long", 10000000000L).withDouble("t_double", 1000.85d).withFloat("t_float", 15.50f)
                .withDateTime("t_date", new Date()).withBytes("t_bytes", "some data in bytes".getBytes(StandardCharsets.UTF_8))
                .build().toString().getBytes().length;

    }

    @Assessor
    public long estimateSize() {
        return "{id:1000, name:\"somename\"}".getBytes().length * config.getRowCount();
    }

    @Split
    public List<RowGeneratorMapper> split(@PartitionSize final long bundles) {
        long nbBundle = Math.max(1, estimateSize() / bundles);
        final long bundleCount = Math.max(1, bundles / recordSize);
        final int totalData = config.getRowCount();
        return LongStream.range(0, nbBundle).mapToObj(i -> {
            final int from = (int) (bundleCount * i);
            final int to = (i == nbBundle - 1) ? totalData : (int) (from + bundleCount);
            if (to == 0) {
                return null;
            }
            final RowGeneratorSource.Config dataSetChunk = new RowGeneratorSource.Config();
            dataSetChunk.setStart(from);
            dataSetChunk.setRowCount(to);
            dataSetChunk.setStringPrefix(config.getStringPrefix());
            dataSetChunk.setWithNullValues(config.isWithNullValues());
            dataSetChunk.setStringPrefix(config.getStringPrefix());
            dataSetChunk.setWithBytes(config.isWithBytes());
            dataSetChunk.setWithMissingIdEvery(config.getWithMissingIdEvery());
            return new RowGeneratorMapper(dataSetChunk, recordBuilderFactory);
        }).filter(Objects::nonNull).collect(toList());
    }

    @Emitter
    public RowGeneratorSource createWorker() {
        return new RowGeneratorSource(config, recordBuilderFactory);
    }

}
