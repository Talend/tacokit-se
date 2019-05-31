package org.talend.components.jdbc;

import java.sql.SQLException;

public final class ErrorFactory {

    private ErrorFactory() {
    }

    public static IllegalStateException toIllegalStateException(final Throwable e) {
        if (!(e instanceof SQLException)) {
            return new IllegalStateException(e);
        }

        final IllegalStateException error = new IllegalStateException(e.getMessage());
        error.setStackTrace(e.getStackTrace());
        return error;

    }
}
