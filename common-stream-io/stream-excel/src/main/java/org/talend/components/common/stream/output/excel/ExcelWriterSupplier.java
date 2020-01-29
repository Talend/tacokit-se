package org.talend.components.common.stream.output.excel;

import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterRepository;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.api.output.WritableTarget;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.ExcelConfiguration;
import org.talend.components.common.stream.format.JsonConfiguration;

public class ExcelWriterSupplier implements RecordWriterSupplier<Workbook> {

    static {
        RecordWriterRepository.getInstance().put(ExcelConfiguration.class, new ExcelWriterSupplier());
    }

    @Override
    public RecordWriter getWriter(WritableTarget<Workbook> target, ContentFormat config) {
        assert config instanceof ExcelConfiguration : "excel writer not with excel config";
        final ExcelConfiguration excelConfig = (ExcelConfiguration) config;

        return new ExcelWriter(excelConfig, target);
    }
}
