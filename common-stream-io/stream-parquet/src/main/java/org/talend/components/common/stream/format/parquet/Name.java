package org.talend.components.common.stream.format.parquet;

import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class Name {

    private static char separator = '!';

    @Getter
    private final String name;

    @Getter
    private final String rawName;

    public static Name fromParquetName(final String parquetName) {
        final int posSep = parquetName.indexOf(Name.separator);
        if (posSep < 0) {
            return new Name(parquetName, null);
        }
        String rawName = parquetName.substring(0, posSep);
        if (rawName.length() == 0) {
            rawName = null;
        }
        String name = parquetName.substring(posSep + 1);
        if (name.length() == 0) {
            name = null;
        }
        return new Name(name, rawName);
    }

    public String parquetName() {
        return Optional.ofNullable(this.rawName).orElse("") + Name.separator + Optional.ofNullable(this.name).orElse("");
    }
}
