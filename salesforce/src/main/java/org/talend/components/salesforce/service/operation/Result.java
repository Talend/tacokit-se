package org.talend.components.salesforce.service.operation;

import java.util.ArrayList;
import java.util.List;

public class Result {

    public static final Result OK = new Result(null);

    private final List<String> errors;

    public Result(List<String> errorsInput) {
        this.errors = new ArrayList<>(errorsInput != null ? errorsInput.size() : 0);
        if (errorsInput != null) {
            this.errors.addAll(errorsInput);
        }
    }

    public Iterable<String> getErrors() {
        return this.errors;
    }

    public boolean isOK() {
        return this.errors.isEmpty();
    }
}
