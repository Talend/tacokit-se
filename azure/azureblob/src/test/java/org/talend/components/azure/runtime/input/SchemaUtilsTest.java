package org.talend.components.azure.runtime.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SchemaUtilsTest {

    @Test
    void correct() {
        Set<String> previous = new HashSet<>();
        String res = SchemaUtils.correct("Hello", 1, previous);
        Assertions.assertEquals("Hello",res);


        previous.add("Hello");
        res = SchemaUtils.correct("Hello", 1, previous);
        Assertions.assertEquals("Hello1",res);

        res = SchemaUtils.correct("2name?!special zz ", 1, previous);
        Assertions.assertEquals("_name__special_zz_",res);
        previous.add("_name__special_zz_");
        previous.add("_name__special_zz_1");
        previous.add("_name__special_zz_2");

        res = SchemaUtils.correct("2name?!special zz ", 1, previous);
        Assertions.assertEquals("_name__special_zz_3",res);


    }
}