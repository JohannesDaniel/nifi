package org.apache.nifi.xml;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.Test;
import org.mockito.Mockito;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelectors;
import org.xmlunit.matchers.CompareMatcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.nifi.xml.ArrayWrapping.NO_WRAPPING;
import static org.apache.nifi.xml.ArrayWrapping.USE_PROPERTY_AS_WRAPPER;
import static org.apache.nifi.xml.ArrayWrapping.USE_PROPERTY_FOR_ELEMENTS;
import static org.apache.nifi.NullSuppression.ALWAYS_SUPPRESS;
import static org.apache.nifi.NullSuppression.NEVER_SUPPRESS;
import static org.apache.nifi.NullSuppression.SUPPRESS_MISSING;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.DATE_FORMAT;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.TIMESTAMP_FORMAT;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.TIME_FORMAT;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getEmptyNestedRecordDefinedSchema;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getEmptyNestedRecordEmptyNestedSchema;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getEmptyRecordsWithEmptySchema;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getNestedRecords;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getNestedRecordsWithOnlyNullValues;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getNestedRecordsWithNullValues;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getRecordWithSimpleArray;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getRecordWithSimpleMap;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getSimpleRecords;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getSimpleRecordsWithChoice;
import static org.apache.nifi.xml.TestWriteXMLResultUtils.getSimpleRecordsWithNullValues;
import static org.junit.Assert.assertThat;

public class TestWriteXMLResult {

    // test data types

    @Test
    public void testSimpleRecord() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecords();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testSimpleRecordWithNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testSimpleRecordWithNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME></NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME></NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testSimpleRecordWithNullValuesSuppressMissings() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, SUPPRESS_MISSING, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME></NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testEmptyRecordWithEmptySchema() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getEmptyRecordsWithEmptySchema();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME></NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        System.out.println(out);

        assertThat(xmlResult, CompareMatcher.isIdenticalTo(out.toString()).ignoreWhitespace());
    }

    @Test
    public void testNestedRecord() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecords();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET>292 West Street</STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<STREET>123 6th St.</STREET><CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testNestedRecordWithNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testNestedRecordWithNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET></STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<STREET></STREET><CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testNestedRecordWithNullValuesSuppressMissings() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, SUPPRESS_MISSING, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET></STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testNestedRecordWithOnlyNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getNestedRecordsWithOnlyNullValues();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testNestedRecordWithOnlyNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getNestedRecordsWithOnlyNullValues();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><ADDRESS><STREET></STREET><CITY></CITY></ADDRESS>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ADDRESS><STREET></STREET><CITY></CITY></ADDRESS>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testEmptyNestedRecordEmptySchemaNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getEmptyNestedRecordEmptyNestedSchema();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><ADDRESS></ADDRESS><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ADDRESS></ADDRESS><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testEmptyNestedRecordEmptySchemaAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getEmptyNestedRecordEmptyNestedSchema();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testNestedEmptyRecordDefinedSchemaSuppressMissing() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getEmptyNestedRecordDefinedSchema();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, SUPPRESS_MISSING, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><ADDRESS></ADDRESS><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ADDRESS></ADDRESS><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleArray() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        System.out.println(out);
        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleArrayWithNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleArrayWithNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN>Tom</CHILDREN><CHILDREN></CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN>Tom</CHILDREN><CHILDREN></CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleArrayWithOnlyNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.ONLY_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testEmptyArray() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testEmptyArrayNeverSupressPropAsWrapper() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, USE_PROPERTY_AS_WRAPPER, "ARRAY", "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><ARRAY></ARRAY><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ARRAY></ARRAY><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleArrayPropAsWrapper() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, USE_PROPERTY_AS_WRAPPER, "ARRAY", "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><ARRAY><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN></ARRAY>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ARRAY><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN></ARRAY>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleArrayPropForElem() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, USE_PROPERTY_FOR_ELEMENTS, "ELEM", "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN><ELEM>Tom</ELEM><ELEM>Anna</ELEM><ELEM>Ben</ELEM></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><ELEM>Tom</ELEM><ELEM>Anna</ELEM><ELEM>Ben</ELEM></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleMapAlwaysSuppressWithoutNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2>Anna</CHILD2></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2>Anna</CHILD2></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleMapAlwaysSuppressHasNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleMapAlwaysSuppressOnlyNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.ONLY_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleMapAlwaysSuppressEmpty() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleMapNeverSuppressHasNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2></CHILD2></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2></CHILD2></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testSimpleMapNeverSuppressEmpty() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN></CHILDREN><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN></CHILDREN><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testChoice() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithChoice();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }


    /*
    *
    *
    * Test writeRawRecord
    *
    *
     */


    @Test
    public void testWriteWithoutSchemaSimpleRecord() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecords();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleRecordWithNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleRecordWithNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME></NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleRecordWithNullValuesSuppressMissings() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getSimpleRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, SUPPRESS_MISSING, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME></NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaNestedRecord() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecords();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET>292 West Street</STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<STREET>123 6th St.</STREET><CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaNestedRecordWithNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaNestedRecordWithNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        RecordSet recordSet = getNestedRecordsWithNullValues();
        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY>" +
                "<ADDRESS><STREET></STREET><CITY>Jersey City</CITY></ADDRESS></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY><ADDRESS>" +
                "<CITY>Seattle</CITY></ADDRESS></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaNestedRecordWithOnlyNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getNestedRecordsWithOnlyNullValues();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaNestedRecordWithOnlyNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getNestedRecordsWithOnlyNullValues();

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><ADDRESS><STREET></STREET><CITY></CITY></ADDRESS>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ADDRESS></ADDRESS>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleArray() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        System.out.println(out);
        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleArrayWithNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN>Tom</CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleArrayWithNullValuesNeverSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><CHILDREN>Tom</CHILDREN><CHILDREN></CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN>Tom</CHILDREN><CHILDREN></CHILDREN><CHILDREN>Ben</CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleArrayWithOnlyNullValuesAlwaysSuppress() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.ONLY_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaEmptyArray() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaEmptyArrayNeverSupressPropAsWrapper() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, USE_PROPERTY_AS_WRAPPER, "ARRAY", "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><ARRAY></ARRAY><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ARRAY></ARRAY><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleArrayPropAsWrapper() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, USE_PROPERTY_AS_WRAPPER, "ARRAY", "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><ARRAY><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN></ARRAY>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><ARRAY><CHILDREN>Tom</CHILDREN><CHILDREN>Anna</CHILDREN><CHILDREN>Ben</CHILDREN></ARRAY>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleArrayPropForElem() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleArray(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, USE_PROPERTY_FOR_ELEMENTS, "ELEM", "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><CHILDREN><ELEM>Tom</ELEM><ELEM>Anna</ELEM><ELEM>Ben</ELEM></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><ELEM>Tom</ELEM><ELEM>Anna</ELEM><ELEM>Ben</ELEM></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleMapAlwaysSuppressWithoutNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.WITHOUT_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2>Anna</CHILD2></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2>Anna</CHILD2></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleMapAlwaysSuppressHasNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.write(recordSet);
        writer.flush();

        String xmlResult = "<ROOT><PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleMapAlwaysSuppressOnlyNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.ONLY_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleMapAlwaysSuppressEmpty() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, ALWAYS_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleMapNeverSuppressHasNull() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.HAS_NULL);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2></CHILD2></CHILDREN>" +
                "<NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN><CHILD1>Tom</CHILD1><CHILD3>Ben</CHILD3><CHILD2></CHILD2></CHILDREN>" +
                "<NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }

    @Test
    public void testWriteWithoutSchemaSimpleMapNeverSuppressEmpty() throws IOException {
        OutputStream out = new ByteArrayOutputStream();

        RecordSet recordSet = getRecordWithSimpleMap(TestWriteXMLResultUtils.NullValues.EMPTY);

        WriteXMLResult writer = new WriteXMLResult(Mockito.mock(ComponentLog.class), recordSet.getSchema(), new SchemaNameAsAttribute(),
                out, true, NEVER_SUPPRESS, NO_WRAPPING, null, "ROOT", DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT);

        writer.onBeginRecordSet();

        Record record;
        while ((record = recordSet.next()) != null) {
            writer.writeRawRecord(record);
        }

        writer.onFinishRecordSet();
        writer.flush();
        writer.close();

        String xmlResult = "<ROOT><PERSON><CHILDREN></CHILDREN><NAME>Cleve Butler</NAME><AGE>42</AGE><COUNTRY>USA</COUNTRY></PERSON>" +
                "<PERSON><CHILDREN></CHILDREN><NAME>Ainslie Fletcher</NAME><AGE>33</AGE><COUNTRY>UK</COUNTRY></PERSON></ROOT>";

        assertThat(xmlResult, CompareMatcher.isSimilarTo(out.toString()).ignoreWhitespace().withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
    }
}
