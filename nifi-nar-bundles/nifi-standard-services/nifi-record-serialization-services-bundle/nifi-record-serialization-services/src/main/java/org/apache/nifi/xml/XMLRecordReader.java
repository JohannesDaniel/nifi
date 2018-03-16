package org.apache.nifi.xml;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.Tuple;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class XMLRecordReader implements RecordReader {

    // nested records mit feldern mit gleichem namen wie root
    // alles auf final, ist allerdings nur wirklich wichtig bei XMLReader, bei XMLRecordReader offenbar nicht

    private InputStream in;
    private RecordSchema schema;
    private List<String> fieldNames;
    private String rootName;
    private String recordName;

    private StartElement currentRecordStartTag;

    private XMLEventReader xmlEventReader;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;


    // final zu allem wo es geht

    public XMLRecordReader(InputStream in, RecordSchema schema, boolean isArray, String xmlPathToRecord,
                           final String dateFormat, final String timeFormat, final String timestampFormat) throws MalformedRecordException {
        this.in =  in;
        this.schema = schema;
        this.fieldNames = schema.getFieldNames();

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        boolean evaluateXmlPathToRecord = xmlPathToRecord != null;
        if (evaluateXmlPathToRecord) {

        }

        try {
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            xmlEventReader = xmlInputFactory.createXMLEventReader(in);
            // was wenn leer?
            // bei isArray muss das Root-Tag weggeparsed werden
            if (isArray) {
                StartElement rootTag = getNextStartTag();
                setNextRecordStartTag();
                rootName = rootTag.getName().toString();
                if (currentRecordStartTag != null) {
                    recordName = currentRecordStartTag.getName().toString();
                }
            } else {
                StartElement rootTag = getNextStartTag();
                rootName = getNextStartTag().getName().toString();
                recordName = rootName;
                currentRecordStartTag = rootTag;
            }

        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }

    }

    private StartElement getNextStartTag() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                return xmlEvent.asStartElement();
            }
        }
        return null;
    }

    private void setNextRecordStartTag() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if (startElement.getName().toString().equals(recordName)) {
                    currentRecordStartTag = startElement;
                } else {
                    skipElement();
                }
            }
        }
        currentRecordStartTag = null;
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (currentRecordStartTag == null) {
            return null;
        }

        System.out.println("Start record");
        try {
            Record record = parseRecord(currentRecordStartTag, this.schema);
            setNextRecordStartTag();

            System.out.println("Return record");
            return record;
        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }


        /*
        try {

            while(xmlEventReader.hasNext()){
                XMLEvent xmlEvent = xmlEventReader.nextEvent();

                if (xmlEvent.isStartElement()) {
                    StartElement startElement = xmlEvent.asStartElement();
                    System.out.println(startElement.getName());
                    parseRecord(startElement);


                } else if (xmlEvent.isEndElement()) {
                    EndElement endElement = xmlEvent.asEndElement();
                    System.out.println(endElement.getName());
                }
            }
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
*/
    }

    private Record parseRecord(StartElement startElement, RecordSchema schema) throws XMLStreamException {
        Map<String, Object> recordValues = new HashMap<String, Object>();
        //Record record = new MapRecord(schema, recordValues);


        // parse attributes
        Iterator iterator = startElement.getAttributes();
        while (iterator.hasNext())
        {
            Attribute attribute = (Attribute) iterator.next();
            //System.out.println(att.getName() + " " + att.getValue());
        }

        // parse fields

        while(xmlEventReader.hasNext()){
            XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                StartElement startSubElement = xmlEvent.asStartElement();
                Optional<RecordField> field = schema.getField(startSubElement.getName().toString());
                if (field.isPresent()){
                    // parse simple field

                    // parse array

                    // parse record

                    System.out.println(startSubElement.getName());
                } else {
                    skipElement();
                }
            } else if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if (endElement.getName().equals(startElement.getName())) {
                    return null;
                }
            }
        }
        return null;
        /*
            List<RecordField> fields = new ArrayList<RecordField>() {{
                add(new RecordField("field_int", RecordFieldType.INT.getDataType()));
                add(new RecordField("field_string", RecordFieldType.STRING.getDataType()));
            }};

            Map<String, Object> values = new HashMap<String, Object>(){{
                put("field_int", 123);
                put("field_string", "value_string");
            }};

            return new MapRecord(new SimpleRecordSchema(fields), values);
         */
    }

    private void skipElement() throws XMLStreamException {
        while(xmlEventReader.hasNext()){
            XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                skipElement();
            }
            if (xmlEvent.isEndElement()) {
                return;
            }
        }
    }

    private Object parseSimpleField(StartElement startElement) throws XMLStreamException {
        while(xmlEventReader.hasNext()){
            XMLEvent xmlEvent = xmlEventReader.nextEvent();


        }


        return new RecordField("", null);
    }





    @Override
    public RecordSchema getSchema() {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
