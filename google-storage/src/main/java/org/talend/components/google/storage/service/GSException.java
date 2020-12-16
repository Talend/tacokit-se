package org.talend.components.google.storage.service;

import org.talend.sdk.component.api.exception.ComponentException;

public class GSException extends ComponentException {

    private static final long serialVersionUID = -848195802193096133L;

    public GSException(ErrorOrigin errorOrigin, String message) {
        super(errorOrigin, message);
    }

    public GSException(ErrorOrigin errorOrigin, String message, Throwable cause) {
        super(errorOrigin, message, cause);
    }
}
