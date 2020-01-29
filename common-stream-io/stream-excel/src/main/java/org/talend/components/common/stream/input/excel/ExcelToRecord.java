package org.talend.components.common.stream.input.excel;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ExcelToRecord {

    /** record facotry */
    private final RecordBuilderFactory recordBuilderFactory;

    private Schema schema;

    static class Column {
        final String name;

        CellType type;

        final int index;

        public Column(String name, CellType type, int index) {
            this.name = name;
            this.type = type;
            this.index = index;
        }

        CellType getRealCellType(Row record) {
            // update type if null (init with header)
            if (this.type == null) {
                final Cell currentCell = record.getCell(this.index);
                this.type = currentCell.getCellType();
            }

            // return real cell type (in case of formula);
            CellType cellType = this.type;
            if (this.type == CellType.FORMULA) {
                Cell cell = record.getCell(this.index);
                cellType = cell.getCachedFormulaResultType();
            }
            return cellType;
        }
    }

    private List<Column> columns = null;

    public ExcelToRecord(RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
    }

    public Record toRecord(Row record) {
        if (schema == null) {
            inferSchema(record, false);
        }

        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder();

        for (int i = 0; i < schema.getEntries().size(); i++) {
            final Cell recordCell = record.getCell(i);
            final String colName = this.columns.get(i).name;
            switch (schema.getEntries().get(i).getType()) {
            case BOOLEAN:
                recordBuilder.withBoolean(colName, recordCell.getBooleanCellValue());
                break;
            case DOUBLE:
                recordBuilder.withDouble(colName, recordCell.getNumericCellValue());
                break;
            default:
                recordBuilder.withString(colName, recordCell.getStringCellValue());
            }
        }

        return recordBuilder.build();
    }

    public Schema inferSchema(Row record, boolean isHeader) {
        if (schema == null) {
            if (this.columns == null) {
                this.columns = inferSchemaColumns(record, isHeader);
            }
            if (isHeader) {
                return null;
            }

            Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
            for (Column column : this.columns) {
                Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
                entryBuilder.withName(column.name);
                CellType cellType = column.getRealCellType(record);

                switch (cellType) {
                    case ERROR:
                        throw new UnsupportedOperationException("Error cell exists in excel document in the " + (column.index + 1) + " column");
                    case STRING:
                        entryBuilder.withType(Schema.Type.STRING);
                        break;
                    case NUMERIC:
                        entryBuilder.withType(Schema.Type.DOUBLE);
                        break;
                    case BOOLEAN:
                        entryBuilder.withType(Schema.Type.BOOLEAN);
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unexpected cell type:%s", cellType.name()));
                }
                schemaBuilder.withEntry(entryBuilder.build());
            }
            schema = schemaBuilder.build();
        }
        return schema;
    }


    public List<Column> inferSchemaColumns(Row excelRecord, boolean isHeader) {
        return StreamSupport.stream(excelRecord.spliterator(), false)
                .filter(Objects::nonNull)
                .map((Cell cell) -> this.buildColumn(cell, isHeader))
                .collect(Collectors.toList());
    }

    private Column buildColumn(Cell cell, boolean isHeader) {
        final String columnName;
        if (isHeader) {
            columnName = cell.getStringCellValue();
        } else {
            columnName = "field" + cell.getColumnIndex();
        }
        final CellType cellType = cell.getCellType();
        return new Column(columnName, isHeader ? null : cellType, cell.getColumnIndex());
    }
}
