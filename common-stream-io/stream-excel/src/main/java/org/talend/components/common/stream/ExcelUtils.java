/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream;

import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.talend.components.common.stream.format.excel.ExcelConfiguration.ExcelFormat;

public class ExcelUtils {

    public static Workbook createWorkBook(ExcelFormat format) {
        return format == ExcelFormat.EXCEL97 ? new HSSFWorkbook() : new XSSFWorkbook();
    }

    public static Workbook readWorkBook(ExcelFormat format, InputStream input) throws IOException {
        if (format == ExcelFormat.EXCEL97) {
            return new HSSFWorkbook(input);
        }
        if (format == ExcelFormat.EXCEL2007) {
            return new XSSFWorkbook(input);
        }
        return new HSSFWorkbook(input);
    }
}
