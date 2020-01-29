package org.talend.components.common.stream.output.excel;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.stream.api.output.WritableTarget;

public class ExcelTarget implements WritableTarget<Workbook> {

    private final OutputStream out;

    public ExcelTarget(OutputStream out) {
        this.out = out;
    }

    @Override
    public void write(Workbook wb) throws IOException {
        wb.write(out);
        wb.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
