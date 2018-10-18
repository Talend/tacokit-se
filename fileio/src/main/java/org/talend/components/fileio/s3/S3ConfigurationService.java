package org.talend.components.fileio.s3;

import org.talend.components.simplefileio.SimpleFileIODatasetProperties;
import org.talend.components.simplefileio.SimpleFileIOFormat;
import org.talend.components.simplefileio.s3.S3DatasetProperties;
import org.talend.components.simplefileio.s3.S3DatastoreProperties;
import org.talend.components.simplefileio.s3.S3Region;
import org.talend.components.simplefileio.s3.input.S3InputProperties;
import org.talend.components.simplefileio.s3.output.S3OutputProperties;
import org.talend.sdk.component.api.service.Service;

// todo: drop that when dropping the tcomp0 dependency and embrassing tacokit programming model
@Service
public class S3ConfigurationService {

    public S3OutputProperties toOutputConfiguration(final S3OutputConfig configuration) {
        final S3OutputProperties properties = new S3OutputProperties("name");
        properties.setDatasetProperties(createDataSet(configuration.getDataset()));
        properties.mergeOutput.setValue(configuration.isMergeOutput());
        properties.overwrite.setValue(configuration.isOverwrite());
        return properties;
    }

    public S3InputProperties toInputConfiguration(final S3DataSet configuration) {
        final S3InputProperties properties = new S3InputProperties("name");
        properties.setDatasetProperties(createDataSet(configuration));
        properties.limit.setValue(configuration.getLimit());
        return properties;
    }

    private S3DatasetProperties createDataSet(final S3DataSet configuration) {
        final S3DatastoreProperties dataStore = new S3DatastoreProperties("datastoreRef");
        dataStore.specifyCredentials.setValue(configuration.getDatastore().isSpecifyCredentials());
        dataStore.accessKey.setValue(configuration.getDatastore().getAccessKey());
        dataStore.secretKey.setValue(configuration.getDatastore().getSecretKey());

        final S3DatasetProperties dataset = new S3DatasetProperties("datasetRef");
        dataset.bucket.setValue(configuration.getBucket());
        dataset.object.setValue(configuration.getObject());
        dataset.fieldDelimiter
                .setValue(SimpleFileIODatasetProperties.FieldDelimiterType.valueOf(configuration.getFieldDelimiter().name()));
        dataset.specificFieldDelimiter.setValue(configuration.getSpecificFieldDelimiter());
        dataset.recordDelimiter
                .setValue(SimpleFileIODatasetProperties.RecordDelimiterType.valueOf(configuration.getRecordDelimiter().name()));
        dataset.format.setValue(SimpleFileIOFormat.valueOf(configuration.getFormat().name()));
        dataset.encryptDataAtRest.setValue(configuration.isEncryptDataAtRest());
        dataset.encryptDataInMotion.setValue(configuration.isEncryptDataInMotion());
        dataset.kmsForDataAtRest.setValue(configuration.getKmsForDataAtRest());
        dataset.kmsForDataInMotion.setValue(configuration.getKmsForDataInMotion());
        dataset.setDatastoreProperties(dataStore);
        return dataset;
    }
}
