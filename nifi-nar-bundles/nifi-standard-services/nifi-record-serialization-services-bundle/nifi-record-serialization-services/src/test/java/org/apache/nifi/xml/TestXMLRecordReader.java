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
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestXMLRecordReader {
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

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

    @Test
    public void testParseEmptyArray() throws IOException, MalformedRecordException {
        InputStream is = new ByteArrayInputStream("<root></root>".getBytes());
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", dateFormat, timeFormat, timestampFormat);
        assertEquals(reader.nextRecord(true, true), null);
    }

    @Test
    public void test() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader rr = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", dateFormat, timeFormat, timestampFormat);
        rr.nextRecord(true, true);
    }

    @Test
    public void testParsingSimpleRecords() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        XMLRecordReader reader = new XMLRecordReader(is, getSimpleSchema(), true, "PERSON", dateFormat, timeFormat, timestampFormat);
        Assert.assertArrayEquals(new Object[] {"Cleve Butler", 42, "USA"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Ainslie Fletcher", 33, "UK"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Amélie Bonfils", 74, "FR"}, reader.nextRecord().getValues());
        Assert.assertArrayEquals(new Object[] {"Elenora Scrivens", 16, "USA"}, reader.nextRecord().getValues());
    }

    @Test
    public void testBrokenXml() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/cds_broken.xml");
        XMLRecordReader rr = new XMLRecordReader(is, getSimpleSchema(), true, null, dateFormat, timeFormat, timestampFormat);
        rr.nextRecord(true, true);
    }



    @Test
    public void testNestedXml() throws IOException, MalformedRecordException {
        InputStream is = new FileInputStream("src/test/resources/xml/people_nested.xml");
        RecordSchema schema = getSchemaWithNestedRecord();


        XMLRecordReader rr = new XMLRecordReader(is, schema, true, "PERSON", dateFormat, timeFormat, timestampFormat);


        rr.nextRecord(true, true);
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
        final DataType dataType = RecordFieldType.RECORD.getRecordDataType(getNestedSchema());
        fields.add(new RecordField("ADDRESS", dataType));
        return new SimpleRecordSchema(fields);
    }

    private RecordSchema nestedSchemaWithArray() {
        return null;
    }

}
