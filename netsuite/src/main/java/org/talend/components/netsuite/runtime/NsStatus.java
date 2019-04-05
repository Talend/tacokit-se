package org.talend.components.netsuite.runtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NsStatus {

    private boolean isSuccess;

    private List<Detail> details;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Detail {

        private Type type;

        private String code;

        private String message;
    }

    public enum Type {
        ERROR,
        WARN,
        INFO
    }
}
