package org.talend.components.jdbc.output;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.talend.sdk.component.api.record.Record;

/**
 * Rejected record with reject reason
 */
@Getter
@AllArgsConstructor
@RequiredArgsConstructor
public class Reject {

    private final String msg;

    private String sqlState;

    private Integer errorCode;

    private final Record record;

    @Override
    public String toString() {
        return "{\"sqlState\": \"" + sqlState + "\", \"errorCode\":" + errorCode + ", \"msg\": \"" + msg + "\", \"record\":"
                + record + "}";
    }
}
