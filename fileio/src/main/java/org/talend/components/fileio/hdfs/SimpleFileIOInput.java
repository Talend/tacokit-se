package org.talend.components.fileio.hdfs;

import static org.talend.sdk.component.api.component.Icon.IconType.FILE_HDFS_O;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.talend.components.simplefileio.runtime.SimpleFileIOInputRuntime;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.meta.Documentation;

@Version
@Icon(FILE_HDFS_O)
@PartitionMapper(name = "SimpleFileIOInput")
@Documentation("This component reads data from HDFS.")
public class SimpleFileIOInput extends PTransform<PBegin, PCollection<IndexedRecord>> {

    private final SimpleFileIODataSet configuration;

    private final SimpleFileIOConfigurationService service;

    public SimpleFileIOInput(@Option("configuration") final SimpleFileIODataSet configuration,
            final SimpleFileIOConfigurationService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @Override
    public PCollection<IndexedRecord> expand(final PBegin input) {
        final SimpleFileIOInputRuntime runtime = new SimpleFileIOInputRuntime();
        runtime.initialize(null, service.toInputConfiguration(configuration));
        return runtime.expand(input);
    }
}
