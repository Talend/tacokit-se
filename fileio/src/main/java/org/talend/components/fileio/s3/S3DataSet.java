package org.talend.components.fileio.s3;

import static org.talend.sdk.component.api.component.Icon.IconType.FILE_S3_O;

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
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@Icon(FILE_S3_O)
@DataSet("S3DataSet")
@Documentation("Dataset of a S3 source.")
@OptionsOrder({ "datastore", "region", "unknownRegion", "bucket", "object", "encryptDataAtRest", "kmsForDataAtRest", "format",
        "recordDelimiter", "specificRecordDelimiter", "fieldDelimiter", "specificFieldDelimiter", "textEnclosureCharacter",
        "escapeCharacter", "excelFormat", "sheet", "encoding4CSV", "encoding4EXCEL", "specificEncoding4CSV",
        "specificEncoding4EXCEL", "setHeaderLine4CSV", "setHeaderLine4EXCEL", "headerLine4CSV", "headerLine4EXCEL",
        "setFooterLine4EXCEL", "footerLine4EXCEL", "limit" })
public class S3DataSet implements Serializable {

    @Option
    @Documentation("The S3 datastore")
    private S3DataStore datastore;

    @Option
    @Documentation("The region to use.")
    private S3Region region = S3Region.DEFAULT;

    @Option
    @ActiveIf(target = "region", value = "OTHER")
    @Documentation("A custom region.")
    private String unknownRegion;

    @Option
    @Required
    @Suggestable(value = "S3FindBuckets", parameters = { "datastore", "region", "unknownRegion" })
    @Documentation("The dataset bucket.")
    private String bucket;

    @Option
    @Required
    @Documentation("The dataset object.")
    private String object;

    // not yet active
    // @Option
    // @Documentation("Should data in motion be encrypted.")
    private boolean encryptDataInMotion;

    // not yet active
    // @Option
    // @Documentation("KMS to use for data in motion encryption.")
    private String kmsForDataInMotion;

    @Option
    @Documentation("Should data at rest be encrypted.")
    private boolean encryptDataAtRest;

    @Option
    @ActiveIf(target = "encryptDataAtRest", value = "true")
    @Documentation("KMS to use for data at rest encryption.")
    private String kmsForDataAtRest;

    @Option
    @Required
    @Documentation("KMS to use for data at rest encryption.")
    private SimpleFileIOFormat format = SimpleFileIOFormat.CSV;

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

    @Getter
    @AllArgsConstructor
    public enum S3Region {
        DEFAULT("us-east-1"),
        AP_SOUTH_1("ap-south-1"),
        AP_SOUTHEAST_1("ap-southeast-1"),
        AP_SOUTHEAST_2("ap-southeast-2"),
        AP_NORTHEAST_1("ap-northeast-1"),
        AP_NORTHEAST_2("ap-northeast-2"),
        // http://docs.amazonaws.cn/en_us/general/latest/gr/rande.html#cnnorth_region
        CN_NORTH_1("cn-north-1"),
        EU_WEST_1("eu-west-1"),
        EU_WEST_2("eu-west-2"),
        EU_CENTRAL_1("eu-central-1"),
        // http://docs.aws.amazon.com/govcloud-us/latest/UserGuide/using-govcloud-endpoints.html
        GovCloud("us-gov-west-1"),
        CA_CENTRAL_1("ca-central-1"),
        SA_EAST_1("sa-east-1"),
        US_EAST_1("us-east-1"),
        US_EAST_2("us-east-2"),
        US_WEST_1("us-west-1"),
        US_WEST_2("us-west-2"),
        OTHER("us-east-1");

        private String value;

        public static S3Region fromString(String region) {
            for (S3Region s3Region : S3Region.values()) {
                if (s3Region.getValue().equalsIgnoreCase(region)) {
                    return s3Region;
                }
            }
            return null;
        }

        /**
         * Refer to this table to know the mapping between region/location/endpoint
         * http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
         *
         */
        public static S3Region fromLocation(String location) {
            switch (location) {
            case "US": // refer to BucketLocationUnmarshaller
                return S3Region.US_EAST_1;
            case "EU":
                return S3Region.EU_WEST_1;
            default:
                return fromString(location); // do not need to convert

            }
        }

        /**
         * Refer to this table to know the mapping between region/location/endpoint
         * http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
         *
         * @return
         */
        public String toEndpoint() {
            switch (this) {
            case GovCloud:
                return "s3-us-gov-west-1.amazonaws.com";
            case CN_NORTH_1:
                return "s3.cn-north-1.amazonaws.com.cn";
            default:
                // Do not need special logical for us-east-1 by using dualstack, and it support IPv6 & IPv4
                return String.format("s3.dualstack.%s.amazonaws.com", value);
            }
        }
    }
}
