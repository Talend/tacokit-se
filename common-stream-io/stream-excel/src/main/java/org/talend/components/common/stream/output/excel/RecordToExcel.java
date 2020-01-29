package org.talend.components.common.stream.output.excel;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

/**
 * Transform record format to excel format.
 */
public class RecordToExcel {

    /**
     * Build excel row from record.
     * @param constructor : row builder.
     * @param record : record for read values.
     * @return excel row.
     */
    public Row from(Supplier<Row> constructor, Record record) {
        final Row row = constructor.get();
        final List<Entry> entries = record.getSchema().getEntries();

        for (Schema.Entry entry : entries) {
            final Cell cell = this.addCell(row);
            this.majCellValue(cell, record, entry);
        }
        return row;
    }

    /**
     * build excel header from record schema.
     * @param constructor : row builder.
     * @param schema: input schema.
     * @return excel row with title.
     */
    public Row buildHeader(Supplier<Row> constructor, Schema schema) {
        final Row headerRow = constructor.get();
        schema.getEntries()
                .forEach((Entry e) -> this.buildHeaderCell(headerRow, e));
        return headerRow;
    }

    /**
     * build cell from row & entry.
     * @param headerRow : excel row for header.
     * @param entry : record schema entry.
     */
    private void buildHeaderCell(Row headerRow, Schema.Entry entry) {
        final Cell headerCell = addCell(headerRow);
        headerCell.setCellValue(entry.getName());
    }

    /**
     * Simply add a cell to a row.
     * @param row : row it add a cell.
     * @return added cell.
     */
    private Cell addCell(Row row) {
        return row.createCell(row.getPhysicalNumberOfCells());
    }

    private void majCellValue(Cell cell,
            Record record,
            Schema.Entry entry) {

        final String entityName = entry.getName();
        switch (entry.getType()) {
        case BOOLEAN:
            cell.setCellType(CellType.BOOLEAN);
            cell.setCellValue(record.getBoolean(entityName));
            break;
        case DATETIME:
            cell.setCellType(CellType.NUMERIC);
            cell.setCellValue(Date.from(record.getDateTime(entityName).toInstant()));
            break;
        case INT:
            cell.setCellType(CellType.NUMERIC);
            cell.setCellValue(record.getInt(entityName));
            break;
        case LONG:
            cell.setCellType(CellType.NUMERIC);
            cell.setCellValue(record.getLong(entityName));
            break;
        case FLOAT:
            cell.setCellType(CellType.NUMERIC);
            cell.setCellValue(record.getFloat(entityName));
            break;
        case DOUBLE:
            cell.setCellType(CellType.NUMERIC);
            cell.setCellValue(record.getDouble(entityName));
            break;
        case BYTES:
            cell.setCellType(CellType.STRING);
            cell.setCellValue(Arrays.toString(record.getBytes(entityName)));
            break;
        default:
            cell.setCellType(CellType.STRING);
            cell.setCellValue(String
                    .valueOf(record.get(Object.class, entry.getName())));
        }
    }
}
