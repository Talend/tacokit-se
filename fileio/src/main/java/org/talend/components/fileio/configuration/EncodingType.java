package org.talend.components.fileio.configuration;

public enum EncodingType {

    UTF8("UTF-8"),
    ISO_8859_15("ISO-8859-15"),
    OTHER("Other");

    private final String value;

    private EncodingType(final String value) {
        this.value = value;
    }

    public String getEncoding() {
        return value;
    }

}
