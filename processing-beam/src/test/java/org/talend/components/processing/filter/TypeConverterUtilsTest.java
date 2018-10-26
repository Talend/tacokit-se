// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.processing.filter;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.junit.Test;

public class TypeConverterUtilsTest {

    private String input_String = "3"; //$NON-NLS-1$

    private String input_String_double = "3.0"; //$NON-NLS-1$

    private String input_String_empty = ""; //$NON-NLS-1$

    private String input_String_default = "0"; //$NON-NLS-1$

    private String input_String_default_double = "0.0"; //$NON-NLS-1$

    private String input_String_default_char = "a"; //$NON-NLS-1$

    private String input_String_default_boolean = "true"; //$NON-NLS-1$

    private String input_String_default_boolean_number = "1"; //$NON-NLS-1$

    private String input_String_null = null;

    private java.nio.ByteBuffer input_ByteBuffer_null = null;

    private byte input_byte = 3;

    private byte input_byte_default = 0;

    private Byte input_Byte = 3;

    private Byte input_Byte_null = null;

    private Byte input_Byte_default = 0;

    private boolean input_boolean = true;

    private boolean input_boolean_default = false;

    private Boolean input_Boolean = true;

    private Boolean input_Boolean_default = false;

    private Boolean input_Boolean_null = null;

    private double input_double = 3.0d;

    private double input_double_default = 0d;

    private Double input_Double = 3.0D;

    private Double input_Double_null = null;

    private Double input_Double_default = 0D;

    private float input_float = 3.0f;

    private float input_float_default = 0f;

    private Float input_Float = 3.0F;

    private Float input_Float_null = null;

    private Float input_Float_default = 0F;

    private BigDecimal input_BigDecimal = new BigDecimal(3);

    // weird error case
    private BigDecimal input_BigDecimal_double = new BigDecimal(((Double) 3.000D).toString());

    private BigDecimal input_BigDecimal_null = null;

    private BigDecimal input_BigDecimal_default = new BigDecimal(0);

    private BigDecimal input_BigDecimal_defaultdouble = BigDecimal.valueOf(0.0d);

    private int input_int = 3;

    private int input_int_default = 0;

    private Integer input_Integer = 3;

    private Integer input_Integer_null = null;

    private Integer input_Integer_default = 0;

    private long input_long = 3l;

    private long input_long_default = 0l;

    private Long input_Long = 3L;

    private Long input_Long_null = null;

    private Long input_Long_default = 0L;

    private short input_short = 3;

    private short input_short_default = 0;

    private Short input_Short = 3;

    private Short input_Short_null = null;

    private Short input_Short_default = 0;

    private char input_char = 'a';

    private char input_char_default = ' ';

    private Character input_Char = 'a';

    private Character input_Char_null = null;

    private Character input_Char_default = ' ';

    @Test
    public void test_String() {
        assertEquals(input_String, TypeConverterUtils.parseToString(input_String));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_byte));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_Byte));
        assertEquals(input_String, TypeConverterUtils.parseToString(java.nio.ByteBuffer.wrap(input_String.getBytes())));
        assertEquals(input_String_double, TypeConverterUtils.parseToString(input_double));
        assertEquals(input_String_double, TypeConverterUtils.parseToString(input_Double));
        assertEquals(input_String_double, TypeConverterUtils.parseToString(input_float));
        assertEquals(input_String_double, TypeConverterUtils.parseToString(input_Float));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_BigDecimal));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_int));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_Integer));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_long));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_Long));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_short));
        assertEquals(input_String, TypeConverterUtils.parseToString(input_Short));
        assertEquals(input_String_default_char, TypeConverterUtils.parseToString(input_char));
        assertEquals(input_String_default_char, TypeConverterUtils.parseToString(input_Char));
    }

    @Test
    public void test_String_default() {
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_String_null));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_ByteBuffer_null));
        assertEquals(input_String_default, TypeConverterUtils.parseToString(input_byte_default));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_Byte_null));
        assertEquals(input_String_default_double, TypeConverterUtils.parseToString(input_double_default));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_Double_null));
        assertEquals(input_String_default_double, TypeConverterUtils.parseToString(input_float_default));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_Float_null));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_BigDecimal_null));
        assertEquals(input_String_default, TypeConverterUtils.parseToString(input_int_default));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_Integer_null));
        assertEquals(input_String_default, TypeConverterUtils.parseToString(input_long_default));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_Long_null));
        assertEquals(input_String_default, TypeConverterUtils.parseToString(input_short_default));
        assertEquals(input_String_null, TypeConverterUtils.parseToString(input_Short_null));
    }

    @Test
    public void test_Integer() {
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_String));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_byte));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_Byte));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_double));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_Double));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_float));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_Float));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_BigDecimal));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_int));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_Integer));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_long));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_Long));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_short));
        assertEquals(input_Integer, TypeConverterUtils.parseToInteger(input_Short));
    }

    @Test
    public void test_Integer_default() {
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_String_null));
        assertEquals(input_Integer_default, TypeConverterUtils.parseToInteger(input_byte_default));
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_Byte_null));
        assertEquals(input_Integer_default, TypeConverterUtils.parseToInteger(input_double_default));
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_Double_null));
        assertEquals(input_Integer_default, TypeConverterUtils.parseToInteger(input_float_default));
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_Float_null));
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_BigDecimal_null));
        assertEquals(input_Integer_default, TypeConverterUtils.parseToInteger(input_int_default));
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_Integer_null));
        assertEquals(input_Integer_default, TypeConverterUtils.parseToInteger(input_long_default));
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_Long_null));
        assertEquals(input_Integer_default, TypeConverterUtils.parseToInteger(input_short_default));
        assertEquals(input_Integer_null, TypeConverterUtils.parseToInteger(input_Short_null));
    }

    @Test
    public void test_Byte() {
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_String));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_byte));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_Byte));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_double));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_Double));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_float));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_Float));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_BigDecimal));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_int));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_Integer));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_long));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_Long));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_short));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_Short));
        assertEquals(input_Byte, TypeConverterUtils.parseToByte(input_Short));
        assertEquals(Byte.valueOf((byte) 0), TypeConverterUtils.parseToByte(false));
        assertEquals(Byte.valueOf((byte) 1), TypeConverterUtils.parseToByte(true));
    }

    @Test
    public void test_Byte_default() {
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_String_null));
        assertEquals(input_Byte_default, TypeConverterUtils.parseToByte(input_byte_default));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_Byte_null));
        assertEquals(input_Byte_default, TypeConverterUtils.parseToByte(input_double_default));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_Double_null));
        assertEquals(input_Byte_default, TypeConverterUtils.parseToByte(input_float_default));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_Float_null));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_BigDecimal_null));
        assertEquals(input_Byte_default, TypeConverterUtils.parseToByte(input_int_default));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_Integer_null));
        assertEquals(input_Byte_default, TypeConverterUtils.parseToByte(input_long_default));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_Long_null));
        assertEquals(input_Byte_default, TypeConverterUtils.parseToByte(input_short_default));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_Short_null));
        assertEquals(input_Byte_default, TypeConverterUtils.parseToByte(input_boolean_default));
        assertEquals(input_Byte_null, TypeConverterUtils.parseToByte(input_Boolean_null));
    }

    @Test
    public void test_Double() {
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_String));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_byte));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_Byte));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_double));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_Double));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_float));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_Float));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_BigDecimal));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_int));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_Integer));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_long));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_Long));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_short));
        assertEquals(input_Double, TypeConverterUtils.parseToDouble(input_Short));
    }

    @Test
    public void test_Double_default() {
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_String_null));
        assertEquals(input_Double_default, TypeConverterUtils.parseToDouble(input_byte_default));
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_Byte_null));
        assertEquals(input_Double_default, TypeConverterUtils.parseToDouble(input_double_default));
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_Double_null));
        assertEquals(input_Double_default, TypeConverterUtils.parseToDouble(input_float_default));
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_Float_null));
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_BigDecimal_null));
        assertEquals(input_Double_default, TypeConverterUtils.parseToDouble(input_int_default));
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_Integer_null));
        assertEquals(input_Double_default, TypeConverterUtils.parseToDouble(input_long_default));
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_Long_null));
        assertEquals(input_Double_default, TypeConverterUtils.parseToDouble(input_short_default));
        assertEquals(input_Double_null, TypeConverterUtils.parseToDouble(input_Short_null));
    }

    @Test
    public void test_Float() {
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_String));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_byte));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_Byte));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_double));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_Double));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_float));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_Float));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_BigDecimal));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_int));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_Integer));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_long));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_Long));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_short));
        assertEquals(input_Float, TypeConverterUtils.parseToFloat(input_Short));
    }

    @Test
    public void test_Float_default() {
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_String_null));
        assertEquals(input_Float_default, TypeConverterUtils.parseToFloat(input_byte_default));
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_Byte_null));
        assertEquals(input_Float_default, TypeConverterUtils.parseToFloat(input_double_default));
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_Double_null));
        assertEquals(input_Float_default, TypeConverterUtils.parseToFloat(input_float_default));
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_Float_null));
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_BigDecimal_null));
        assertEquals(input_Float_default, TypeConverterUtils.parseToFloat(input_int_default));
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_Integer_null));
        assertEquals(input_Float_default, TypeConverterUtils.parseToFloat(input_long_default));
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_Long_null));
        assertEquals(input_Float_default, TypeConverterUtils.parseToFloat(input_short_default));
        assertEquals(input_Float_null, TypeConverterUtils.parseToFloat(input_Short_null));
    }

    @Test
    public void test_BigDecimal() {
        assertEquals(input_BigDecimal_double, BigDecimal.valueOf(3.0d));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_String));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_byte));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_Byte));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_double));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_Double));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_float));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_Float));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_BigDecimal));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_int));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_Integer));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_long));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_Long));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_short));
        assertEquals(input_BigDecimal, TypeConverterUtils.parseToBigDecimal(input_Short));
    }

    @Test
    public void test_BigDecimal_default() {
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_String_null));
        assertEquals(input_BigDecimal_default, TypeConverterUtils.parseToBigDecimal(input_byte_default));
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_Byte_null));
        assertEquals(input_BigDecimal_default, TypeConverterUtils.parseToBigDecimal(input_double_default));
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_Double_null));
        assertEquals(input_BigDecimal_default, TypeConverterUtils.parseToBigDecimal(input_float_default));
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_Float_null));
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_BigDecimal_null));
        assertEquals(input_BigDecimal_default, TypeConverterUtils.parseToBigDecimal(input_int_default));
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_Integer_null));
        assertEquals(input_BigDecimal_default, TypeConverterUtils.parseToBigDecimal(input_long_default));
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_Long_null));
        assertEquals(input_BigDecimal_default, TypeConverterUtils.parseToBigDecimal(input_short_default));
        assertEquals(input_BigDecimal_null, TypeConverterUtils.parseToBigDecimal(input_Short_null));
    }

    @Test
    public void test_Long() {
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_String));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_byte));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_Byte));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_double));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_Double));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_float));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_Float));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_BigDecimal));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_int));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_Integer));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_long));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_Long));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_short));
        assertEquals(input_Long, TypeConverterUtils.parseToLong(input_Short));
    }

    @Test
    public void test_Long_default() {
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_String_null));
        assertEquals(input_Long_default, TypeConverterUtils.parseToLong(input_byte_default));
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_Byte_null));
        assertEquals(input_Long_default, TypeConverterUtils.parseToLong(input_double_default));
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_Double_null));
        assertEquals(input_Long_default, TypeConverterUtils.parseToLong(input_float_default));
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_Float_null));
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_BigDecimal_null));
        assertEquals(input_Long_default, TypeConverterUtils.parseToLong(input_int_default));
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_Integer_null));
        assertEquals(input_Long_default, TypeConverterUtils.parseToLong(input_long_default));
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_Long_null));
        assertEquals(input_Long_default, TypeConverterUtils.parseToLong(input_short_default));
        assertEquals(input_Long_null, TypeConverterUtils.parseToLong(input_Short_null));
    }

    @Test
    public void test_Short() {
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_String));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_byte));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_Byte));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_double));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_Double));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_float));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_Float));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_BigDecimal));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_int));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_Integer));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_long));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_Long));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_short));
        assertEquals(input_Short, TypeConverterUtils.parseToShort(input_Short));
    }

    @Test
    public void test_Short_default() {
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_String_null));
        assertEquals(input_Short_default, TypeConverterUtils.parseToShort(input_byte_default));
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_Byte_null));
        assertEquals(input_Short_default, TypeConverterUtils.parseToShort(input_double_default));
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_Double_null));
        assertEquals(input_Short_default, TypeConverterUtils.parseToShort(input_float_default));
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_Float_null));
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_BigDecimal_null));
        assertEquals(input_Short_default, TypeConverterUtils.parseToShort(input_int_default));
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_Integer_null));
        assertEquals(input_Short_default, TypeConverterUtils.parseToShort(input_long_default));
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_Long_null));
        assertEquals(input_Short_default, TypeConverterUtils.parseToShort(input_short_default));
        assertEquals(input_Short_null, TypeConverterUtils.parseToShort(input_Short_null));
    }

    @Test
    public void test_Character() {
        assertEquals((Character) input_String.charAt(0), TypeConverterUtils.parseToCharacter(input_String));
        assertEquals(input_Char, TypeConverterUtils.parseToCharacter(input_char));
        assertEquals(input_Char, TypeConverterUtils.parseToCharacter(input_Char));
    }

    @Test
    public void test_Character_default() {
        assertEquals(input_Char_null, TypeConverterUtils.parseToCharacter(input_String_null));
        assertEquals(input_Char_default, TypeConverterUtils.parseToCharacter(input_char_default));
        assertEquals(input_Char_null, TypeConverterUtils.parseToCharacter(input_Char_null));
    }

    @Test
    public void test_Boolean() {
        assertEquals(input_boolean, TypeConverterUtils.parseToBoolean(input_boolean));
        assertEquals(input_Boolean, TypeConverterUtils.parseToBoolean(input_Boolean));
        assertEquals(input_Boolean_default, TypeConverterUtils.parseToBoolean(input_Boolean_default));
        assertEquals(input_Boolean_null, TypeConverterUtils.parseToBoolean(input_Boolean_null));

        assertEquals(input_Boolean_default, TypeConverterUtils.parseToBoolean(input_String));
        assertEquals(input_Boolean, TypeConverterUtils.parseToBoolean(input_String_default_boolean));
        assertEquals(input_Boolean, TypeConverterUtils.parseToBoolean(input_String_default_boolean_number));
        assertEquals(input_Boolean_null, TypeConverterUtils.parseToBoolean(input_String_null));
    }

}
