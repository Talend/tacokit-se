package org.talend.components.fileio.s3;

import static org.talend.sdk.component.api.component.Icon.IconType.FILE_S3_O;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
@Icon(FILE_S3_O)
@DataSet("S3OutputDataSet")
@Documentation("Dataset of a S3 sink.")
@OptionsOrder({ "datastore", "region", "unknownRegion", "bucket", "object", "encryptDataAtRest", "kmsForDataAtRest", "format",
        "recordDelimiter", "specificRecordDelimiter", "fieldDelimiter", "specificFieldDelimiter", "textEnclosureCharacter",
        "escapeCharacter", "excelFormat", "sheet", "encoding4CSV", "encoding4EXCEL", "specificEncoding4CSV",
        "specificEncoding4EXCEL", "setHeaderLine4CSV", "setHeaderLine4EXCEL", "headerLine4CSV", "headerLine4EXCEL",
        "setFooterLine4EXCEL", "footerLine4EXCEL", "overwrite", "mergeOutput", "limit" })
public class S3OutputDataSet extends S3DataSet {

    @Option
    @Documentation("Should overwrite data.")
    private boolean overwrite;

    @Option
    @Documentation("Should merge data.")
    private boolean mergeOutput;
}
