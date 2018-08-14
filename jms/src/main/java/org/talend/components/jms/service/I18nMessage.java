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
