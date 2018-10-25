package org.talend.components.processing.flatten;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;

public class FlattenDoFn extends DoFn<IndexedRecord, IndexedRecord> {

    private FlattenConfiguration properties = null;

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        IndexedRecord inputRecord = context.element();

        String columnToFlatten = properties.getColumnToFlatten();
        String delimiter = properties.getFieldDelimiter();
        boolean isDiscardTrailingEmptyStr = properties.isDiscardTrailingEmptyStr();
        boolean isTrim = properties.isTrim();

        if (FlattenUtils.isNotNullOrEmpty(columnToFlatten)) {

            if (columnToFlatten.startsWith(".")) {
                columnToFlatten = columnToFlatten.substring(1, columnToFlatten.length());
            }

            String[] path = columnToFlatten.split("\\.");

            List<Object> flattenedFields = FlattenUtils.getInputFields(inputRecord, columnToFlatten);

            Schema schema = FlattenUtils.transformSchema(inputRecord.getSchema(), path, 0);

            if (FlattenUtils.isSimpleField(flattenedFields)) {
                flattenedFields = FlattenUtils.delimit(String.valueOf(flattenedFields.get(0)), delimiter,
                        isDiscardTrailingEmptyStr, isTrim);
            }

            for (Object outputValue : flattenedFields) {
                GenericRecord outputRecord = FlattenUtils.generateNormalizedRecord(context.element(),
                        context.element().getSchema(), schema, path, 0, outputValue);
                context.output(outputRecord);
            }
        }
    }

    public FlattenDoFn withConfiguration(FlattenConfiguration properties) {
        this.properties = properties;
        return this;
    }
}
