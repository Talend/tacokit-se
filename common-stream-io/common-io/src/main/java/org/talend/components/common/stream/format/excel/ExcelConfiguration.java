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
package org.talend.components.common.stream.format.excel;

import java.util.Optional;

import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.Encoding;
import org.talend.components.common.stream.format.OptionalLine;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@GridLayout({ @GridLayout.Row("excelFormat"), @GridLayout.Row("sheetName"), @GridLayout.Row("encoding"),
        @GridLayout.Row({ "header" }), // headers
        @GridLayout.Row({ "footer" }) // footers
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {})
@Data
public class ExcelConfiguration implements ContentFormat {

    private static final long serialVersionUID = -4004242251704350965L;

    public enum ExcelFormat {
        EXCEL2007,
        EXCEL97,
        HTML
    }

    @Option
    @Documentation("Excel format.")
    private ExcelFormat excelFormat = ExcelFormat.EXCEL2007;

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("Excel sheet name.")
    @DefaultValue("Sheet1")
    private String sheetName = "Sheet1";

    @Option
    @ActiveIf(target = "excelFormat", value = "HTML")
    @Documentation("Content encoding.")
    private Encoding encoding = new Encoding();

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("Header.")
    private OptionalLine header;

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("Footer.")
    private OptionalLine footer;

    public int calcHeader() {
        return Optional.ofNullable(this.header).map(OptionalLine::getSize).orElse(0);
    }

    public int calcFooter() {
        return Optional.ofNullable(this.footer).map(OptionalLine::getSize).orElse(0);
    }

}
