package org.apache.nifi.xml;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.Test;
import org.mockito.Mockito;
import org.xmlunit.matchers.CompareMatcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.nifi.xml.ArrayWrapping.NO_WRAPPING;
import static org.apache.nifi.xml.NullSuppression.ALWAYS_SUPPRESS;
import static org.apache.nifi.xml.NullSuppression.NEVER_SUPPRESS;
import static org.apache.nifi.xml.NullSuppression.SUPPRESS_MISSING;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.DATE_FORMAT;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.TIMESTAMP_FORMAT;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.TIME_FORMAT;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getNestedRecords;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getNestedRecordsExclusivelyWithNullValues;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getNestedRecordsWithNullValues;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getRecordWithSimpleArray;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getSimpleRecords;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getSimpleRecordsWithNullValues;
import static org.junit.Assert.assertThat;

public class TestWriteXMLResult {



    @Test
    public void testSimpleRecord() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecords();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testSimpleRecordWithNullValuesAlwaysSuppress() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testSimpleRecordWithNullValuesNeverSuppress() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME></NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME></NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testSimpleRecordWithNullValuesSuppressMissings() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, SUPPRESS_MISSING, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME></NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testNestedRecord() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecords();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET>292 West Street</STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<STREET>123 6th St.</STREET><CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testNestedRecordWithNullValuesAlwaysSuppress() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testNestedRecordWithNullValuesNeverSuppress() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET></STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<STREET></STREET><CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testNestedRecordWithNullValuesSuppressMissings() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, SUPPRESS_MISSING, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET></STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testNestedRecordExclusivelyWithNullValues() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getNestedRecordsExclusivelyWithNullValues();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        System.out.println(out.toString());
    }

    @Test
    public void testSimpleArray() throws IOException, MalformedRecordException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        System.out.println(out.toString());
    }





}
