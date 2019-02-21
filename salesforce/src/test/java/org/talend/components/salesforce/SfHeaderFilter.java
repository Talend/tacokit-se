package org.talend.components.salesforce;

import java.util.Locale;
import java.util.function.Predicate;

public class SfHeaderFilter implements Predicate<String> {

    @Override
    public boolean test(final String s) {
        final String header = s.toLowerCase(Locale.ROOT);
        return "content-length".equals(header) || "user-agent".equals(header) || "content-encoding".equals(header)
                || "vary".equals(header) || "set-cookie".equals(header) || "expires".equals(header)
                || "connection".equals(header);
    }
}
