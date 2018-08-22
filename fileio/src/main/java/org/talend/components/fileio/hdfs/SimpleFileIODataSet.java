package org.talend.components.fileio.hdfs;

import static org.talend.sdk.component.api.component.Icon.IconType.FILE_HDFS_O;

import java.io.Serializable;

import org.talend.components.fileio.configuration.EncodingType;
import org.talend.components.fileio.configuration.ExcelFormat;
import org.talend.components.fileio.configuration.FieldDelimiterType;
import org.talend.components.fileio.configuration.RecordDelimiterType;
import org.talend.components.fileio.configuration.SimpleFileIOFormat;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@Icon(FILE_HDFS_O)
@DataSet("SimpleFileIODataSet")
@Documentation("Dataset of a HDFS source.")
@OptionsOrder({ "datastore", "path", "format", "recordDelimiter", "specificRecordDelimiter", "fieldDelimiter",
        "specificFieldDelimiter", "textEnclosureCharacter", "escapeCharacter", "excelFormat", "sheet", "encoding",
        "specificEncoding", "setHeaderLine", "headerLine", "setFooterLine", "footerLine", "limit" })
public class SimpleFileIODataSet implements Serializable {

    @Option
    @Documentation("The datastore to use for that dataset")
    private SimpleFileIODataStore datastore;

    @Option
    @Required
    @Documentation("The file format")
    private SimpleFileIOFormat format = SimpleFileIOFormat.CSV;

    @Option
    @Required
    @Documentation("The file location")
    private String path;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("The record delimiter to split the file in records")
    private RecordDelimiterType recordDelimiter = RecordDelimiterType.LF;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @ActiveIf(target = "recordDelimiter", value = "OTHER")
    @Documentation("A custom delimiter if `recordDelimiter` is `OTHER`")
    private String specificRecordDelimiter = ";";

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("The field delimiter to split the records in columns")
    private FieldDelimiterType fieldDelimiter = FieldDelimiterType.SEMICOLON;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @ActiveIf(target = "fieldDelimiter", value = "OTHER")
    @Documentation("A custom delimiter if `fieldDelimiter` is `OTHER`")
    private String specificFieldDelimiter = ";";

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("Select a encoding type")
    private EncodingType encoding = EncodingType.UTF8;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @ActiveIf(target = "encoding", value = "OTHER")
    @Documentation("Set the custom encoding")
    private String specificEncoding;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("enable the header setting")
    private boolean setHeaderLine;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @ActiveIf(target = "setHeaderLine", value = "true")
    @Documentation("set the header number")
    private String headerLine;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("set the text enclosure character")
    private String textEnclosureCharacter;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("set the escape character")
    private String escapeCharacter;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @Documentation("Select a excel format")
    private ExcelFormat excelFormat = ExcelFormat.EXCEL2007;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @Documentation("set the excel sheet name")
    private String sheet;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @Documentation("enable the footer setting")
    private boolean setFooterLine;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @ActiveIf(target = "setFooterLine", value = "true")
    @Documentation("set the footer number")
    private String footerLine;

    @Option
    @ActiveIf(target = ".", value = "-2147483648")
    @Documentation("Maximum number of data to handle if positive.")
    private int limit = -1;

}
