package org.talend.components.onedrive.common;

public class UnknownAuthenticationTypeException extends RuntimeException {

    public UnknownAuthenticationTypeException() {
        super("Unknown or unset Authentication type");
    }
}
