package org.apache.nifi.xml;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestXMLRecordReader {
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    // recordName - validate? rootName?
    // check for empty content
    // coerced / unknown
    // records in array
    // exception handling
    // test for schema-xml mismatches
    // finalize
    // what happens for <testtag/>
    // test deeply nested tags & arrays (min 2 level each)


    @Test
    public void testGetRootAndRecord() {
        // to implement
    }

    @Test(expected = MalformedRecordException.class)
    public void testEmptyStream() throws IOException, MalformedRecordException {
        InputStream is = new ByteArrayInputStream(new byte[0]);
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        reader.nextRecord(true, true);
    }

    @Test(expected = MalformedRecordException.class)
    public void testInvalidXml() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/cds_broken.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, null, null, dateFormat, timeFormat, timestampFormat);
        reader.nextRecord(true, true);
    }

    @Test
    public void testParseEmptyArray() throws IOException, MalformedRecordException {
        InputStream is = new ByteArrayInputStream("<root></root>".getBytes());
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        assertEquals(reader.nextRecord(true, true), null);
    }

    @Test
    public void testSimpleRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, reader.nextRecord().getValues());
    }

    @Test
    public void testSimpleRecordIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_no_attributes.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", "42", "USA"}, reader.nextRecord(false, false).getValues());
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", "33", "UK"}, reader.nextRecord(false, false).getValues());
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", "74", "FR"}, reader.nextRecord(false, false).getValues());
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", "16", "USA"}, reader.nextRecord(false, false).getValues());
    }

    @Test
    public void testSimpleRecordWithAttribute() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        Record first = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Cleve Butler", 42, "USA", "P1"}).containsAll(Arrays.asList(first.getValues())));
        assertEquals("P1", first.getAsString("ID"));
        Record second = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Ainslie Fletcher", 33, "UK", "P2"}).containsAll(Arrays.asList(second.getValues())));
        assertEquals("P2", second.getAsString("ID"));
        Record third = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Amélie Bonfils", 74, "FR", "P3"}).containsAll(Arrays.asList(third.getValues())));
        assertEquals("P3", third.getAsString("ID"));
        Record fourth = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Elenora Scrivens", 16, "USA", "P4"}).containsAll(Arrays.asList(fourth.getValues())));
        assertEquals("P4", fourth.getAsString("ID"));
    }

    @Test
    public void testSimpleRecordWithAttribute2() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), true, "PERSON", "ATTR_", dateFormat, timeFormat, timestampFormat);

        // record.getValues() will here also contain a null value as record.getValue("ID") is null
        Record first = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Cleve Butler", 42, "USA", null}).containsAll(Arrays.asList(first.getValues())));
        assertEquals("P1", first.getAsString("ATTR_ID"));
        Record second = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Ainslie Fletcher", 33, "UK", null}).containsAll(Arrays.asList(second.getValues())));
        assertEquals("P2", second.getAsString("ATTR_ID"));
        Record third = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Amélie Bonfils", 74, "FR", null}).containsAll(Arrays.asList(third.getValues())));
        assertEquals("P3", third.getAsString("ATTR_ID"));
        Record fourth = reader.nextRecord();
        assertTrue(Arrays.asList(new Object[] {"Elenora Scrivens", 16, "USA", null}).containsAll(Arrays.asList(fourth.getValues())));
        assertEquals("P4", fourth.getAsString("ATTR_ID"));
    }

    @Test
    public void testSimpleRecordWithAttribute3() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(Collections.emptyList()), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        Record first = reader.nextRecord(true, true);
        assertEquals(null, first.getAsString("ID"));
        Record second = reader.nextRecord(false, false);
        assertEquals("P2", second.getAsString("ID"));
        Record third = reader.nextRecord(true, false);
        assertEquals("P3", third.getAsString("ID"));
        Record fourth = reader.nextRecord(false, true);
        assertEquals(null, fourth.getAsString("ID"));
    }

    // type for integer ?? test for coerceTypes?

    @Test
    public void testSimpleRecordWithAttributeIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        Record first = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Cleve Butler", "42", "USA", "P1"}).containsAll(Arrays.asList(first.getValues())));
        assertEquals("P1", first.getAsString("ID"));
        Record second = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Ainslie Fletcher", "33", "UK", "P2"}).containsAll(Arrays.asList(second.getValues())));
        assertEquals("P2", second.getAsString("ID"));
        Record third = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Amélie Bonfils", "74", "FR", "P3"}).containsAll(Arrays.asList(third.getValues())));
        assertEquals("P3", third.getAsString("ID"));
        Record fourth = reader.nextRecord(false, false);
        assertTrue(Arrays.asList(new Object[] {"Elenora Scrivens", "16", "USA", "P4"}).containsAll(Arrays.asList(fourth.getValues())));
        assertEquals("P4", fourth.getAsString("ID"));
    }

    // attribute in record

    @Test
    public void testNestedRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_nested.xml");
        RecordSchema schema = getSchemaWithNestedRecord();
        XMLRecordReader reader = new XMLRecordReader(is, schema, true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        Object[] valuesFirstRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"292 West Street", "Jersey City"},((Record) valuesFirstRecord[valuesFirstRecord.length - 1]).getValues());

        Object[] valuesSecondRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"123 6th St.", "Seattle"},((Record) valuesSecondRecord[valuesSecondRecord.length - 1]).getValues());

        Object[] valuesThirdRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"44 Shirley Ave.", "Los Angeles"},((Record) valuesThirdRecord[valuesThirdRecord.length - 1]).getValues());

        Object[] valuesFourthRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, Arrays.copyOfRange(valuesFourthRecord, 0, valuesFourthRecord.length - 1));
        Assert.assertArrayEquals(new Object[] {"70 Bowman St." , "Columbus"},((Record) valuesFourthRecord[valuesFourthRecord.length - 1]).getValues());
    }

    @Test
    public void testNestedRecordIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_nested.xml");
        RecordSchema schema = getSchemaWithNestedRecord();
        XMLRecordReader reader = new XMLRecordReader(is, schema, true, "PERSON", null, dateFormat, timeFormat, timestampFormat);

        Record first = reader.nextRecord(false, false);
        Object[] valuesFirstRecord = first.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", "42", "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        assertEquals("P1", first.getAsString("ID"));
        Record nestedFirstRecord = (Record) first.getValue("ADDRESS");
        Assert.assertEquals("Jersey City", nestedFirstRecord.getAsString("CITY"));
        Assert.assertEquals("292 West Street", nestedFirstRecord.getAsString("STREET"));

        Record second = reader.nextRecord(false, false);
        Object[] valuesSecondRecord = second.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", "33", "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        assertEquals("P2", second.getAsString("ID"));
        Record nestedSecondRecord = (Record) second.getValue("ADDRESS");
        Assert.assertEquals("Seattle", nestedSecondRecord.getAsString("CITY"));
        Assert.assertEquals("123 6th St.", nestedSecondRecord.getAsString("STREET"));

        Record third = reader.nextRecord(false, false);
        Object[] valuesThirdRecord = third.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", "74", "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        assertEquals("P3", third.getAsString("ID"));
        Record nestedThirdRecord = (Record) third.getValue("ADDRESS");
        Assert.assertEquals("Los Angeles", nestedThirdRecord.getAsString("CITY"));
        Assert.assertEquals("44 Shirley Ave.", nestedThirdRecord.getAsString("STREET"));

        Record fourth = reader.nextRecord(false, false);
        Object[] valuesFourthRecord = fourth.getValues();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", "16", "USA"}, Arrays.copyOfRange(valuesFourthRecord, 0, valuesFourthRecord.length - 1));
        assertEquals("P4", fourth.getAsString("ID"));
        Record nestedFourthRecord = (Record) fourth.getValue("ADDRESS");
        Assert.assertEquals("Columbus", nestedFourthRecord.getAsString("CITY"));
        Assert.assertEquals("70 Bowman St.", nestedFourthRecord.getAsString("STREET"));
    }

    @Test
    public void testSimpleArray() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_array_simple.xml");
        RecordSchema schema = getSchemaWithSimpleArray();
        XMLRecordReader reader = new XMLRecordReader(is, schema, true, "PERSON", null, dateFormat, timeFormat, timestampFormat);

        Record firstRecord = reader.nextRecord();
        Object[] valuesFirstRecord = firstRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Object[] nestedArrayFirstRecord = (Object[]) valuesFirstRecord[valuesFirstRecord.length - 1];
        assertEquals(2, nestedArrayFirstRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2"}, nestedArrayFirstRecord);
        assertNotEquals(null, firstRecord.getValue("CHILD"));

        Record secondRecord = reader.nextRecord();
        Object[] valuesSecondRecord = secondRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        Object[] nestedArraySecondRecord = (Object[]) valuesSecondRecord[valuesSecondRecord.length - 1];
        assertEquals(1, nestedArraySecondRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1"}, nestedArraySecondRecord);
        assertNotEquals(null, secondRecord.getValue("CHILD"));

        Record thirdRecord = reader.nextRecord();
        Object[] valuesThirdRecord = thirdRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Object[] nestedArrayThirdRecord = (Object[]) valuesThirdRecord[valuesThirdRecord.length - 1];
        assertEquals(3, nestedArrayThirdRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2", "child3"}, nestedArrayThirdRecord);
        assertNotEquals(null, thirdRecord.getValue("CHILD"));

        Record valuesFourthRecord = reader.nextRecord();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, Arrays.copyOfRange(valuesFourthRecord.getValues(), 0, valuesFourthRecord.getValues().length - 1));
        assertEquals(null, valuesFourthRecord.getValue("CHILD"));
    }

    @Test
    public void testSimpleArrayIgnoreSchema() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_array_simple.xml");
        RecordSchema schema = getSchemaWithSimpleArray();
        XMLRecordReader reader = new XMLRecordReader(is, schema, true, "PERSON", null, dateFormat, timeFormat, timestampFormat);

        Record first = reader.nextRecord(false, false);
        System.out.println(first);
        Object[] valuesFirstRecord = first.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", "42", "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Object[] nestedArrayFirstRecord = (Object[]) valuesFirstRecord[valuesFirstRecord.length - 1];
        assertEquals(2, nestedArrayFirstRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2"}, nestedArrayFirstRecord);
        assertNotEquals(null, first.getValue("CHILD"));

        Record second = reader.nextRecord(false, false);
        Object[] valuesSecondRecord = second.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", "33", "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        String nestedArraySecondRecord = (String) valuesSecondRecord[valuesSecondRecord.length - 1];
        Assert.assertEquals("child1", nestedArraySecondRecord);
        assertNotEquals(null, second.getValue("CHILD"));

        Record third = reader.nextRecord(false, false);
        Object[] valuesThirdRecord = third.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", "74", "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Object[] nestedArrayThirdRecord = (Object[]) valuesThirdRecord[valuesThirdRecord.length - 1];
        assertEquals(3, nestedArrayThirdRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2", "child3"}, nestedArrayThirdRecord);
        assertNotEquals(null, third.getValue("CHILD"));

        Record fourth = reader.nextRecord(false, false);
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", "16", "USA"}, Arrays.copyOfRange(fourth.getValues(), 0, fourth.getValues().length - 1));
        assertEquals(null, fourth.getValue("CHILD"));
    }

    @Test
    public void testNestedArrayInNestedRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_array.xml");
        RecordSchema schema = getSchemaWithNestedArray();
        XMLRecordReader reader = new XMLRecordReader(is, schema, true, "PERSON", null, dateFormat, timeFormat, timestampFormat);

        Record firstRecord = reader.nextRecord();
        Object[] valuesFirstRecord = firstRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Record nestedArrayFirstRecord = (Record) firstRecord.getValue("CHILDREN");
        assertEquals(2, ((Object[]) nestedArrayFirstRecord.getValue("CHILD")).length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2"}, ((Object[]) nestedArrayFirstRecord.getValue("CHILD")));

        Record secondRecord = reader.nextRecord();
        Object[] valuesSecondRecord = secondRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        Record nestedArraySecondRecord = (Record) secondRecord.getValue("CHILDREN");
        assertEquals(1, ((Object[]) nestedArraySecondRecord.getValue("CHILD")).length);
        Assert.assertArrayEquals(new Object[] {"child1"}, ((Object[]) nestedArraySecondRecord.getValue("CHILD")));

        Record thirdRecord = reader.nextRecord();
        Object[] valuesThirdRecord = thirdRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Record nestedArrayThirdRecord = (Record) thirdRecord.getValue("CHILDREN");
        assertEquals(3, ((Object[]) nestedArrayThirdRecord.getValue("CHILD")).length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2", "child3"}, ((Object[]) nestedArrayThirdRecord.getValue("CHILD")));

        Record fourthRecord = reader.nextRecord();
        Object[] valuesFourthRecord = fourthRecord.getValues();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, Arrays.copyOfRange(valuesFourthRecord, 0, valuesFourthRecord.length - 1));
        Record nestedArrayFourthRecord = (Record) fourthRecord.getValue("CHILDREN");
        Assert.assertEquals(null, nestedArrayFourthRecord.getValue("CHILD"));
    }

    @Test
    public void testDeeplyNestedArraysAndRecords() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_complex.xml");

    }


    @Test
    public void testDeeplyNestedArraysAndRecordsIgnoreSchema() throws IOException, MalformedRecordException {
        // minimum number of levels of nesting for arrays and records: 2
        InputStream is = new FileInputStream("src/test/resources/xml/people_complex.xml");
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(Collections.emptyList()), true, "PERSON", null, dateFormat, timeFormat, timestampFormat);
        Record first = reader.nextRecord(false, false);
        assertEquals("1", first.getValue("ID"));
        assertEquals("Lisa", first.getValue("NAME"));
        assertEquals("grandmother", first.getValue("ROLE"));
        Object[] gm_arr = (Object[]) first.getValue("CHILDREN");
        assertEquals(2, gm_arr.length);

        Record gm_hus1_arr_rec = (Record) gm_arr[0];
        assertEquals("husband1", gm_hus1_arr_rec.getValue("SPOUSE"));
        Object[] gm_hus1_arr_rec_arr = (Object[]) gm_hus1_arr_rec.getValue("CHILD");
        assertEquals(2, gm_hus1_arr_rec_arr.length);

        Record child1_1 = (Record) gm_hus1_arr_rec_arr[0];
        assertEquals("1-1", child1_1.getValue("ID"));
        assertEquals("Anna", child1_1.getValue("NAME"));
        assertEquals("mother", child1_1.getValue("ROLE"));

        Record child1_1_rec = (Record) child1_1.getValue("CHILDREN");
        assertEquals("first husband", child1_1_rec.getValue("ID"));
        Object[] child1_1_rec_arr = (Object[]) child1_1_rec.getValue("CHILD");
        assertEquals(2, child1_1_rec_arr.length);

        Record child1_1_1 = (Record) child1_1_rec_arr[0];
        assertEquals("1-1-1", child1_1_1.getValue("ID"));
        assertEquals("Selina", child1_1_1.getValue("NAME"));
        assertEquals("daughter", child1_1_1.getValue("ROLE"));

        Record child1_1_2 = (Record) child1_1_rec_arr[1];
        assertEquals("1-1-2", child1_1_2.getValue("ID"));
        assertEquals("Hans", child1_1_2.getValue("NAME"));
        assertEquals("son", child1_1_2.getValue("ROLE"));

        Record child1_2 = (Record) gm_hus1_arr_rec_arr[1];
        assertEquals("1-2", child1_2.getValue("ID"));
        assertEquals("Catrina", child1_2.getValue("NAME"));
        assertEquals("mother", child1_2.getValue("ROLE"));

        Record gm_hus2_arr_rec = (Record) gm_arr[1];
        assertEquals("husband2", gm_hus2_arr_rec.getValue("SPOUSE"));
        Record child1_3 = (Record) gm_hus2_arr_rec.getValue("CHILD");
        assertEquals("1-3", child1_3.getValue("ID"));
        assertEquals("Anna2", child1_3.getValue("NAME"));
        assertEquals("mother", child1_3.getValue("ROLE"));
        assertEquals(2, ((Object[])((Record) child1_3.getValue("CHILDREN")).getValue("CHILD")).length);

        Record second = reader.nextRecord(false, false);
        assertEquals("2-1-1", ((Record)((Record)((Record)((Record) second.getValue("CHILDREN"))
                .getValue("CHILD"))
                .getValue("CHILDREN"))
                .getValue("CHILD"))
                .getValue("ID"));
    }

    private List<RecordField> getSimpleRecordFields () {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("NAME", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("AGE", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("COUNTRY", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private List<RecordField> getNestedRecordFields () {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("STREET", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("CITY", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private RecordSchema getSimpleSchema() {
        return new SimpleRecordSchema(getSimpleRecordFields());
    }

    private RecordSchema getNestedSchema() {
        return new SimpleRecordSchema(getNestedRecordFields());
    }

    private RecordSchema getSchemaWithNestedRecord() {
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(getNestedSchema());
        fields.add(new RecordField("ADDRESS", recordType));
        return new SimpleRecordSchema(fields);
    }

    private RecordSchema getSchemaWithSimpleArray() {
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        fields.add(new RecordField("CHILD", arrayType));
        return new SimpleRecordSchema(fields);
    }

    private RecordSchema getSchemaWithNestedArray() {
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        final List<RecordField> nestedArrayField = new ArrayList<RecordField>() {{ add(new RecordField("CHILD", arrayType)); }};

        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(nestedArrayField));
        fields.add(new RecordField("CHILDREN", recordType));
        return new SimpleRecordSchema(fields);
    }

    private List<RecordField> getSimpleFieldsForComplexData() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("NAME", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("ROLE", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private RecordSchema getSchemaForComplexData() {
        final List<RecordField> fields = getSimpleFieldsForComplexData();

        /*
        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(nestedArrayField));
        fields.add(new RecordField("CHILDREN", recordType));
        */

        final DataType grandchildren = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(getSimpleFieldsForComplexData()));
        final DataType grandchildren_arr = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getDataType());



        /*
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        final List<RecordField> nestedArrayField = new ArrayList<RecordField>() {{ add(new RecordField("CHILD", arrayType)); }};

        final DataType recordType = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(nestedArrayField));
        fields.add(new RecordField("CHILDREN", recordType));
        */
        return new SimpleRecordSchema(fields);
    }

}
