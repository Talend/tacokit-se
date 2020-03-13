/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.azure.runtime.converters;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;

public class ExcelConverter implements RecordConverter<Row> {

    private RecordBuilderFactory builderFactory;

    private Schema schema;

    @Getter
    private List<String> columnNames;

    @Getter
    private List<CellType> columnTypes;

    private Sheet sheet;

    private ExcelConverter(RecordBuilderFactory builderFactory) {
        this.builderFactory = builderFactory;
    }

    /**
     * Create excel converter for input
     *
     * @param builderFactory framework service should be injected
     * @return new instance of ExcelConverter could be used for input component
     */
    public static ExcelConverter of(RecordBuilderFactory builderFactory) {
        return new ExcelConverter(builderFactory);
    }

    /**
     * Create excel converter for output
     *
     * @param currentSheet sheet with rows
     * @return new instance of ExcelConverter could be used for output component
     */
    public static ExcelConverter ofOutput(Sheet currentSheet) {
        ExcelConverter converterForOutput = of(null);
        converterForOutput.sheet = currentSheet;

        return converterForOutput;
    }

    /**
     * @param record
     * @return
     */
    @Override
    public Schema inferSchema(Row record) {
        if (schema == null) {
            if (columnNames == null) {
                columnNames = inferSchemaNames(record, false);
            }

            if (columnTypes == null) {
                columnTypes = inferSchemaTypes(record);
            }

            Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(Schema.Type.RECORD);
            for (int i = 0; i < columnNames.size(); i++) {
                Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
                entryBuilder.withName(columnNames.get(i));
                CellType cellType = columnTypes.get(i);
                if (cellType == CellType.FORMULA) {
                    Cell cell = record.getCell(i);
                    cellType = cell.getCachedFormulaResultTypeEnum();

                }
                switch (cellType) {
                case ERROR:
                    throw new BlobRuntimeException("Error cell exists in excel document in the " + i + 1 + " column");
                case STRING:
                    entryBuilder.withType(Schema.Type.STRING);
                    break;
                case NUMERIC:
                    entryBuilder.withType(Schema.Type.DOUBLE);
                    break;
                case BOOLEAN:
                    entryBuilder.withType(Schema.Type.BOOLEAN);
                }
                schemaBuilder.withEntry(entryBuilder.build());
            }
            schema = schemaBuilder.build();
        }
        return schema;
    }

    private List<CellType> inferSchemaTypes(Row record) {
        Iterator<Cell> cellIterator = record.cellIterator();
        List<CellType> cellTypes = new ArrayList<>();

        while (cellIterator.hasNext()) {
            cellTypes.add(cellIterator.next().getCellTypeEnum());
        }

        return cellTypes;
    }

    public List<String> inferSchemaNames(Row next, boolean isHeader) {
        List<String> columns = new ArrayList<>();
        // TODO check it works correctly
        for (int i = 0; i < next.getLastCellNum(); i++) {
            Cell cell = next.getCell(i);
            if (cell == null)
                continue;

            String columnName;
            if (isHeader) {
                columnName = cell.getStringCellValue();
            } else {
                columnName = "field" + i;
            }

            columns.add(columnName);
        }
        columnNames = columns;
        return columns;
    }

    @Override
    public Record toRecord(Row record) {
        if (schema == null) {
            inferSchema(record);
        }

        Record.Builder recordBuilder = builderFactory.newRecordBuilder();

        for (int i = 0; i < schema.getEntries().size(); i++) {
            String fileName = columnNames.get(i);
            Optional<Cell> recordCellOptional = Optional.ofNullable(record.getCell(i));
            switch (schema.getEntries().get(i).getType()) {

            case BOOLEAN:
                recordCellOptional.ifPresent(cell -> recordBuilder.withBoolean(fileName, cell.getBooleanCellValue()));
                break;
            case DOUBLE:
                recordCellOptional.ifPresent(cell -> recordBuilder.withDouble(fileName, cell.getNumericCellValue()));
                break;
            default:
                recordCellOptional.ifPresent(cell -> recordBuilder.withString(fileName, cell.getStringCellValue()));
            }
        }

        return recordBuilder.build();
    }

    @Override
    public Row fromRecord(Record record) {
        return null;
    }

    public void appendBatchToTheSheet(List<Record> batch, int startRowNumber) {
        if (sheet == null) {
            throw new IllegalStateException("Excel converter wasn't initialized correctly");
        }

        for (int i = startRowNumber; i < batch.size() + startRowNumber; i++) {
            Row row = sheet.createRow(i);
            Record currentRecord = batch.get(i - startRowNumber);
            for (int j = 0; j < currentRecord.getSchema().getEntries().size(); j++) {
                Cell cell = row.createCell(j);
                String entityName = currentRecord.getSchema().getEntries().get(j).getName();
                switch (currentRecord.getSchema().getEntries().get(j).getType()) {
                case BOOLEAN:
                    cell.setCellType(CellType.BOOLEAN);
                    cell.setCellValue(currentRecord.getBoolean(entityName));
                    break;
                case DATETIME:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(Date.from(currentRecord.getDateTime(entityName).toInstant()));
                    break;
                case INT:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getInt(entityName));
                    break;
                case LONG:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getLong(entityName));
                    break;
                case FLOAT:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getFloat(entityName));
                    break;
                case DOUBLE:
                    cell.setCellType(CellType.NUMERIC);
                    cell.setCellValue(currentRecord.getDouble(entityName));
                    break;
                case BYTES:
                    cell.setCellType(CellType.STRING);
                    cell.setCellValue(Arrays.toString(currentRecord.getBytes(entityName)));
                    break;
                default:
                    cell.setCellType(CellType.STRING);
                    cell.setCellValue(String
                            .valueOf(currentRecord.get(Object.class, currentRecord.getSchema().getEntries().get(j).getName())));
                }
            }
        }
    }

    public List<String> inferRecordColumns(Record record) {
        List<String> columnNames = new ArrayList<>();
        record.getSchema().getEntries().forEach(entry -> columnNames.add(entry.getName()));
        return columnNames;
    }
}
