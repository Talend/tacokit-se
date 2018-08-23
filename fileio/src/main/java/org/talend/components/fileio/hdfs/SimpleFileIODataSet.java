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
import org.talend.sdk.component.api.configuration.action.Suggestable;
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
        "specificFieldDelimiter", "textEnclosureCharacter", "escapeCharacter", "excelFormat", "sheet", "encoding4CSV",
        "encoding4EXCEL", "specificEncoding4CSV", "specificEncoding4EXCEL", "setHeaderLine4CSV", "setHeaderLine4EXCEL",
        "headerLine4CSV", "headerLine4EXCEL", "setFooterLine4EXCEL", "footerLine4EXCEL", "limit" })
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
    @Documentation("Select a encoding type for CSV")
    private EncodingType encoding4CSV = EncodingType.UTF8;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @ActiveIf(target = "encoding4CSV", value = "OTHER")
    @Documentation("Set the custom encoding for CSV")
    private String specificEncoding4CSV;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @ActiveIf(target = "excelFormat", value = "HTML")
    @Documentation("Select a encoding type for EXCEL")
    private EncodingType encoding4EXCEL = EncodingType.UTF8;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @ActiveIf(target = "excelFormat", value = "HTML")
    @ActiveIf(target = "encoding4EXCEL", value = "OTHER")
    @Documentation("Set the custom encoding for EXCEL")
    private String specificEncoding4EXCEL;

    // FIXME how to support the logic :
    // show if format is csv or excel
    // now skip it to split the option to two : setHeaderLine4CSV, setHeaderLine4EXCEL

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @Documentation("enable the header setting for CSV")
    private boolean setHeaderLine4CSV;

    @Option
    @ActiveIf(target = "format", value = "CSV")
    @ActiveIf(target = "setHeaderLine4CSV", value = "true")
    @Documentation("set the header number for CSV")
    private String headerLine4CSV;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @Documentation("enable the header setting for EXCEL")
    private boolean setHeaderLine4EXCEL;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @ActiveIf(target = "setHeaderLine4EXCEL", value = "true")
    @Documentation("set the header number for EXCEL")
    private String headerLine4EXCEL;

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
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("set the excel sheet name")
    private String sheet;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @Documentation("enable the footer setting for EXCEL")
    private boolean setFooterLine4EXCEL;

    @Option
    @ActiveIf(target = "format", value = "EXCEL")
    @ActiveIf(target = "setFooterLine4EXCEL", value = "true")
    @Documentation("set the footer number for EXCEL")
    private String footerLine4EXCEL;

    @Option
    @ActiveIf(target = ".", value = "-2147483648")
    @Documentation("Maximum number of data to handle if positive.")
    private int limit = -1;

    /*
     * @Option
     * 
     * @Suggestable(value = "findOptions", parameters = { "datastore", "format" })
     * 
     * @Documentation("a test for the trigger action")
     * private String myoptions;
     */

}
