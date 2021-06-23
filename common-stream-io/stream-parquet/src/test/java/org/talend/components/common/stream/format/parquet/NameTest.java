package org.talend.components.common.stream.format.parquet;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NameTest {

    @ParameterizedTest
    @CsvSource({"hello,", "hello,world", ",world"})
    void parquetName(final String name, final String rawname) {
        final Name pn = new Name(name, rawname);
        final Name pname = Name.fromParquetName(pn.parquetName());
        Assertions.assertEquals(pn , pname);
    }


    @ParameterizedTest
    @CsvSource({"hello!", "hello!world", "!world"})
    void fromParquetName(final String pname) {
        final Name name = Name.fromParquetName(pname);
        Assertions.assertEquals(pname , name.parquetName());
    }
}