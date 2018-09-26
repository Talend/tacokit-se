package org.talend.components.jdbc.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

    String errorDriverDeregister(final String type);

    String warnDriverClose(final String type);

    String errorDriverLoad(String driverId, String missingJars);

    String errorEmptyJdbcURL();

    String errorCantLoadDriver(String dbType);

    String errorUnsupportedSubProtocol();

    String errorInvalidConnection();

    String errorDriverNotFound(String dbType);

    String errorSQL(int errorCode, String message);

    String errorDriverInstantiation(String message);

    String successConnection();

    String errorUnauthorizedQuery();

    String errorEmptyQuery();

    String warnResultSetCantBeClosed();

    String warnStatementCantBeClosed();

    String warnConnectionCantBeClosed();

    String warnReadOnlyOptimisationFailure();

}
