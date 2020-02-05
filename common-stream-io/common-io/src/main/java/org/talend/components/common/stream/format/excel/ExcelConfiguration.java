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
package org.talend.components.common.stream.format.excel;

import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.Encoding;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@GridLayout({ @GridLayout.Row("excelFormat"), @GridLayout.Row("sheetName"), @GridLayout.Row("encoding"),
        @GridLayout.Row({ "useHeader", "header" }), // headers
        @GridLayout.Row({ "useFooter", "footer" }) // footers
})
@Data
public class ExcelConfiguration implements ContentFormat {

    public enum ExcelFormat {
        EXCEL2007,
        EXCEL97,
        HTML
    }

    @Option
    @Documentation("Excel format")
    private ExcelFormat excelFormat = ExcelFormat.EXCEL2007;

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("excel sheet name")
    private String sheetName;

    @Option
    @ActiveIf(target = "excelFormat", value = "HTML")
    @Documentation("content encoding")
    private Encoding encoding = new Encoding();

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("true if some header is present")
    private boolean useHeader;

    @Option
    @ActiveIfs(operator = ActiveIfs.Operator.AND, value = { @ActiveIf(target = "useHeader", value = "true"),
            @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" }) })
    @Documentation("number of header line")
    private int header = 1;

    public int headerSize() {
        if (this.useHeader) {
            return this.header;
        }
        return 0;
    }

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("true if some footer is present")
    private boolean useFooter;

    @Option
    @ActiveIfs(operator = ActiveIfs.Operator.AND, value = { @ActiveIf(target = "useFooter", value = "true"),
            @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" }) })
    @Documentation("number of footer line")
    private int footer = 1;

    public int footerSize() {
        if (this.useFooter) {
            return this.footer;
        }
        return 0;
    }
}
