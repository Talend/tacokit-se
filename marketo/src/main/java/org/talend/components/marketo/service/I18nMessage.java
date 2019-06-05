// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

    String connectionSuccessful();

    String invalidOperation();

    String accessTokenRetrievalError(int code, String additionnalMessage);

    String exceptionOccured(String message);

    String nonManagedType(String type, String field);

    String invalidBlankProperty();

    String invalidFields();

    String periodAgo1w();

    String periodAgo2w();

    String periodAgo1m();

    String periodAgo3m();

    String periodAgo6m();

    String periodAgo1y();

    String periodAgo2y();

    String invalidDateTime();
}
