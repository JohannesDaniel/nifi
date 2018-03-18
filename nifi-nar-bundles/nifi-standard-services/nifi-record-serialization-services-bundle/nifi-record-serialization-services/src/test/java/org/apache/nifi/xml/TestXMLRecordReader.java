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


    @Test
    public void testGetRootAndRecord() {
        // to implement
    }

    @Test(expected = MalformedRecordException.class)
    public void testEmptyStream() throws IOException, MalformedRecordException {
        InputStream is = new ByteArrayInputStream(new byte[0]);
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", dateFormat, timeFormat, timestampFormat);
        reader.nextRecord(true, true);
    }

    @Test(expected = MalformedRecordException.class)
    public void testInvalidXml() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/cds_broken.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, null, dateFormat, timeFormat, timestampFormat);
        reader.nextRecord(true, true);
    }

    @Test
    public void testParseEmptyArray() throws IOException, MalformedRecordException {
        InputStream is = new ByteArrayInputStream("<root></root>".getBytes());
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", dateFormat, timeFormat, timestampFormat);
        assertEquals(reader.nextRecord(true, true), null);
    }

    @Test
    public void testSimpleRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", dateFormat, timeFormat, timestampFormat);
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, reader.nextRecord().getValues());
    }

    @Test
    public void testSimpleRecordWithAttribute() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        List<RecordField> fields = getSimpleRecordFields();
        fields.add(new RecordField("ID", RecordFieldType.STRING.getDataType()));
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(fields), true, "PERSON", dateFormat, timeFormat, timestampFormat);
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
        XMLRecordReader reader = new XMLRecordReader(is, new SimpleRecordSchema(Collections.emptyList()), true, "PERSON", dateFormat, timeFormat, timestampFormat);
        Record first = reader.nextRecord(true, true);
        assertEquals(null, first.getAsString("ID"));
        Record second = reader.nextRecord(false, false);
        assertEquals("P2", second.getAsString("ID"));
        Record third = reader.nextRecord(true, false);
        assertEquals("P3", third.getAsString("ID"));
        Record fourth = reader.nextRecord(false, true);
        assertEquals(null, fourth.getAsString("ID"));
    }

    @Test
    public void testNestedRecord() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_nested.xml");
        RecordSchema schema = getSchemaWithNestedRecord();
        XMLRecordReader reader = new XMLRecordReader(is, schema, true, "PERSON", dateFormat, timeFormat, timestampFormat);
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
    public void testNestedArray() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_array.xml");
        RecordSchema schema = getSchemaWithNestedArray();
        XMLRecordReader reader = new XMLRecordReader(is, schema, true, "PERSON", dateFormat, timeFormat, timestampFormat);

        Object[] valuesFirstRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, Arrays.copyOfRange(valuesFirstRecord, 0, valuesFirstRecord.length - 1));
        Object[] nestedArrayFirstRecord = (Object[]) valuesFirstRecord[valuesFirstRecord.length - 1];
        assertEquals(2, nestedArrayFirstRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2"}, nestedArrayFirstRecord);

        Object[] valuesSecondRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, Arrays.copyOfRange(valuesSecondRecord, 0, valuesSecondRecord.length - 1));
        Object[] nestedArraySecondRecord = (Object[]) valuesSecondRecord[valuesSecondRecord.length - 1];
        assertEquals(1, nestedArraySecondRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1"}, nestedArraySecondRecord);

        Object[] valuesThirdRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, Arrays.copyOfRange(valuesThirdRecord, 0, valuesThirdRecord.length - 1));
        Object[] nestedArrayThirdRecord = (Object[]) valuesThirdRecord[valuesThirdRecord.length - 1];
        assertEquals(3, nestedArrayThirdRecord.length);
        Assert.assertArrayEquals(new Object[] {"child1", "child2", "child3"}, nestedArrayThirdRecord);

        Object[] valuesFourthRecord = reader.nextRecord().getValues();
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, Arrays.copyOfRange(valuesFourthRecord, 0, valuesFourthRecord.length - 1));
        Object[] nestedArrayFourthRecord = (Object[]) valuesFourthRecord[valuesFourthRecord.length - 1];
        assertEquals(0, nestedArrayFourthRecord.length);
        Assert.assertArrayEquals(new Object[] {}, nestedArrayFourthRecord);
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

    private RecordSchema getSchemaWithNestedArray() {
        final List<RecordField> fields = getSimpleRecordFields();
        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        fields.add(new RecordField("CHILDREN", arrayType));
        return new SimpleRecordSchema(fields);
    }

}
