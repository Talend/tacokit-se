// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jms.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

    String errorEmptyURL();

    String errorLoadProvider(String driverId, String missingJars);

    String errorInvalidConnection();

    String errorStartMessagesDelivery();

    String errorCreateJMSInstance();

    String errorInstantiateConnectionFactory(String message);

    String errorCantSendMessage();

    String errorCantReceiveMessage();

    String warnProducerCantBeClosed();

    String warnConsumerCantBeClosed();

    String warnSessionCantBeClosed();

    String warnConnectionCantBeClosed();

    String warnJNDIContextCantBeClosed();

    String successConnection();
}
