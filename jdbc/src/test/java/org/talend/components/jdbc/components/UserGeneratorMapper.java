package org.talend.components.jdbc.components;

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Version
@Icon(Icon.IconType.SAMPLE)
@PartitionMapper(name = "UserGenerator", family = "jdbcTest")
public class UserGeneratorMapper implements Serializable {

    private final UserGeneratorSource.Config config;

    private RecordBuilderFactory recordBuilderFactory;

    public UserGeneratorMapper(@Option("config") final UserGeneratorSource.Config config,
            final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @Assessor
    public long estimateSize() {
        return "{id:1000, name:\"somename\"}".getBytes().length * config.getRowCount();
    }

    @Split
    public List<UserGeneratorMapper> split(@PartitionSize final long bundles) {
        long recordSize = "{id:1000, name:\"somename\"}".getBytes().length;
        long nbBundle = Math.max(1, estimateSize() / bundles);
        final long bundleCount = bundles / recordSize;
        final int totalData = config.getRowCount();
        return LongStream.range(0, nbBundle).mapToObj(i -> {
            final int from = (int) (bundleCount * i);
            final int to = (i == nbBundle - 1) ? totalData : (int) (from + bundleCount);
            if (to == 0) {
                return null;
            }
            final UserGeneratorSource.Config dataSetChunk = new UserGeneratorSource.Config();
            dataSetChunk.setStartIndex(from);
            dataSetChunk.setRowCount(to);
            dataSetChunk.setIdIsNull(config.isIdIsNull());
            dataSetChunk.setNameIsNull(config.isNameIsNull());
            dataSetChunk.setNamePrefix(config.getNamePrefix());
            dataSetChunk.setNullEvery(config.getNullEvery());
            return new UserGeneratorMapper(dataSetChunk, recordBuilderFactory);
        }).filter(Objects::nonNull).collect(toList());
    }

    @Emitter
    public UserGeneratorSource createWorker() {
        return new UserGeneratorSource(config, recordBuilderFactory);
    }

}
