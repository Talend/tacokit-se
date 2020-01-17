package org.talend.components.pubsub.service;

public class PubSubConnectorException extends RuntimeException {


    public PubSubConnectorException() {
        super();
    }

    public PubSubConnectorException(String msg) {
        super(msg);
    }

    public PubSubConnectorException(Throwable cause) {
        super(cause);
    }

    public PubSubConnectorException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
