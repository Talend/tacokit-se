package org.talend.components.localio.fixed;

import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.base.BufferizedProducerSupport;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.beam.spi.record.AvroRecord;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "FixedFlowInputRuntime")
@PartitionMapper(name = "FixedFlowInputRuntime")
@Documentation("This component duplicates an input a configured number of times.")
public class FixedFlowInputRuntime implements Serializable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final FixedFlowInputConfiguration configuration;

    public FixedFlowInputRuntime(@Option("configuration") final FixedFlowInputConfiguration configuration) {
        this.configuration = configuration;
    }

    @Assessor
    public long estimateSize() {
        switch (this.configuration.getOverrideValuesAction()) {
        case NONE:
            return this.configuration.getDataset().getValues().getBytes().length;
        case REPLACE:
            return this.configuration.getOverrideValues().getBytes().length;
        case APPEND:
        default:
            return this.configuration.getDataset().getValues().getBytes().length
                    + this.configuration.getOverrideValues().getBytes().length;
        }
    }

    @Split
    public List<FixedFlowInputRuntime> split(@PartitionSize final long bundles) {
        // No need to split the data at this point because the component will be used with a limited number of records.
        List<FixedFlowInputRuntime> mappers = new ArrayList<>();
        mappers.add(new FixedFlowInputRuntime(configuration));
        return mappers;
    }

    @Emitter
    public FixedFlowInputProducer createWorker() {
        return new FixedFlowInputProducer(configuration);
    }

    class FixedFlowInputProducer implements Serializable {

        private FixedFlowInputConfiguration configuration;

        private BufferizedProducerSupport<IndexedRecord> bufferedReader;

        private int repeat = 0;

        FixedFlowInputProducer(final FixedFlowInputConfiguration configuration) {
            this.configuration = configuration;
        }

        @PostConstruct
        public void init() {
            FixedDataSetRuntime runtime = new FixedDataSetRuntime(configuration.getDataset());
            List<IndexedRecord> values = new LinkedList<>();

            if (configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.NONE
                    || configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.APPEND) {
                if (!configuration.getDataset().getValues().trim().isEmpty()) {
                    values.addAll(runtime.getValues(Integer.MAX_VALUE));
                }
            }

            if (configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.APPEND
                    || configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.REPLACE) {
                configuration.getDataset().setValues(configuration.getOverrideValues());
                if (!configuration.getDataset().getValues().trim().isEmpty()) {
                    values.addAll(runtime.getValues(Integer.MAX_VALUE));
                }
            }

            bufferedReader = new BufferizedProducerSupport<>(() -> {
                if (++repeat > configuration.getRepeat()) {
                    return null;
                } else {
                    return values.iterator();
                }
            });
        }

        @Producer
        public Record next() {
            IndexedRecord record = bufferedReader.next();
            return record != null ? new AvroRecord(record) : null;
        }
    }
}
