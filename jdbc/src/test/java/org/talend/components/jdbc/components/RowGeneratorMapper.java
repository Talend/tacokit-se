/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
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

    public RowGeneratorMapper(@Option("config") final RowGeneratorSource.Config config,
            final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<RowGeneratorMapper> split(@PartitionSize final long bundles) {
        long nbBundle = Math.max(1, config.getRowCount() / 4);
        final long bundleCount = config.getRowCount() / nbBundle;
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
            dataSetChunk.setWithBoolean(config.isWithBoolean());
            return new RowGeneratorMapper(dataSetChunk, recordBuilderFactory);
        }).filter(Objects::nonNull).collect(toList());
    }

    @Emitter
    public RowGeneratorSource createWorker() {
        return new RowGeneratorSource(config, recordBuilderFactory);
    }

}
