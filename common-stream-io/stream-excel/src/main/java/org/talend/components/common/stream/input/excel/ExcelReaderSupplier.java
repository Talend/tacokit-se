package org.talend.components.common.stream.input.excel;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderRepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.ExcelConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ExcelReaderSupplier implements RecordReaderSupplier {

    static {
        RecordReaderRepository.getInstance().put(ExcelConfiguration.class, new ExcelReaderSupplier());
    }

    @Override
    public RecordReader getReader(RecordBuilderFactory factory, ContentFormat config) {
        assert config instanceof ExcelConfiguration : "excel reader not with excel config";

        return new ExcelRecordReader((ExcelConfiguration) config, factory);
    }
}
