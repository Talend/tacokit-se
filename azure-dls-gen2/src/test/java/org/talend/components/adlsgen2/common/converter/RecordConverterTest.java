package org.talend.components.adlsgen2.common.converter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RecordConverterTest {

    @Test
    void getCorrectSchemaFieldName() {

        Assertions.assertEquals("CA_HT",
                RecordConverter.getCorrectSchemaFieldName("CA HT", 0, Collections.emptySet()));

        Assertions.assertEquals("column___Name",
                RecordConverter.getCorrectSchemaFieldName("column?!^Name", 0, Collections.emptySet()));

        Assertions.assertEquals("P1_Vente_Qt_",
                RecordConverter.getCorrectSchemaFieldName("P1_Vente_Qté", 0, Collections.emptySet()));

    }

    @Test
    void getUniqueNameForSchemaField() {

        Assertions.assertEquals("Hello",
                RecordConverter.getUniqueNameForSchemaField("Hello", null));

        Assertions.assertEquals("Hello",
                RecordConverter.getUniqueNameForSchemaField("Hello", Collections.emptySet()));

        Set<String> previous = new HashSet<>();
        previous.add("Hello");
        Assertions.assertEquals("Hello1",
                RecordConverter.getUniqueNameForSchemaField("Hello", previous));

        previous.add("Hello1");
        Assertions.assertEquals("Hello2",
                RecordConverter.getUniqueNameForSchemaField("Hello", previous));

        previous.add("Hello2");
        Assertions.assertEquals("Hello3",
                RecordConverter.getUniqueNameForSchemaField("Hello", previous));

    }
}