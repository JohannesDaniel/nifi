package org.apache.nifi.xml;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.ArrayList;
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
    private String attributePrefix;

    private StartElement currentRecordStartTag;

    private XMLEventReader xmlEventReader;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;


    // final zu allem wo es geht

    public XMLRecordReader(InputStream in, RecordSchema schema, boolean isArray, String recordName,
                           final String dateFormat, final String timeFormat, final String timestampFormat) throws MalformedRecordException {
        this.in =  in;
        this.schema = schema;
        this.fieldNames = schema.getFieldNames();
        this.recordName = recordName;
        this.attributePrefix = attributePrefix;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        try {
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            xmlEventReader = xmlInputFactory.createXMLEventReader(in);
            // was wenn leer?
            // bei isArray muss das Root-Tag weggeparsed werden
            if (isArray) {
                StartElement rootTag = getNextStartTag();
                setNextRecordStartTag();
                rootName = rootTag.getName().toString();

            } else {
                StartElement rootTag = getNextStartTag();
                rootName = getNextStartTag().getName().toString();
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
                if (recordName != null) {
                    if (startElement.getName().toString().equals(recordName)) {
                        currentRecordStartTag = startElement;
                        return;
                    } else {
                        skipElement();
                    }
                } else {
                    currentRecordStartTag = startElement;
                    return;
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
        try {
            Record record = parseRecord(currentRecordStartTag, this.schema, coerceTypes, dropUnknownFields);
            setNextRecordStartTag();
            return record;
        } catch (XMLStreamException e) {
            throw new MalformedRecordException("Could not parse XML", e);
        }
    }

    private Object parseFieldForType(StartElement startElement, String fieldName, DataType dataType) throws XMLStreamException {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                final String value = extractSimpleValue(startElement);
                return DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
            }
            case ARRAY: {
                final DataType arrayDataType = ((ArrayDataType) dataType).getElementType();
                return parseArray(startElement, fieldName, arrayDataType);
            }
            case RECORD: {
                RecordSchema childSchema;
                if (dataType instanceof RecordDataType) {
                    childSchema = ((RecordDataType) dataType).getChildSchema();
                } else {
                    return null;
                }
                return parseRecord(startElement, childSchema, true, true);
            }
            case MAP: {
                // type not supported
                return null;
            }
            case CHOICE: {
                // Type cannot be determined a priori
                return null;
            }
        }
        return null;
    }

    private Object parseAttributeForType(Attribute attribute, String fieldName, DataType dataType) {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                return DataTypeUtils.convertType(attribute.getValue(), dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
            }
        }
        return null;
    }

    private Object[] parseArray(StartElement startElement, String fieldName, DataType arrayDataType) throws XMLStreamException {
        // repeating elements should be sufficient
        List<Object> elements = new ArrayList<>();
        while (xmlEventReader.hasNext()) {
            XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startSubElement = xmlEvent.asStartElement();
                Object element = parseFieldForType(startSubElement, fieldName, arrayDataType);
                elements.add(element);
            } else if (xmlEvent.isEndElement()){
                EndElement endElement = xmlEvent.asEndElement();
                //check not necessary
                if (endElement.getName().equals(startElement.getName())) {
                    break;
                }
            }
        }

        return elements.toArray();
    }

    private String extractSimpleValue(StartElement startElement) throws XMLStreamException {
        // throw exception RecordSchemaForXmlException (extends XMLStreamException) man koennte isCharacters, isEndElement nutzen
        XMLEvent xmlEvent;
        Characters characters;
        xmlEvent = xmlEventReader.nextEvent();
        characters = xmlEvent.asCharacters();
        xmlEventReader.nextEvent();
        return characters.toString();
    }

    private Record parseRecord(StartElement startElement, RecordSchema schema, boolean coerceTypes, boolean dropUnknown) throws XMLStreamException {
        Map<String, Object> recordValues = new HashMap<>();

        // parse attributes
        // field content?
        Iterator iterator = startElement.getAttributes();
        while (iterator.hasNext()) {
            Attribute attribute = (Attribute) iterator.next();
            String fieldName = attribute.getName().toString();

            if (dropUnknown) {
                Optional<RecordField> field = schema.getField(fieldName);
                if (field.isPresent()){
                    if (coerceTypes) {
                        Object value;
                        if ((value = parseAttributeForType(attribute, fieldName, field.get().getDataType())) != null) {
                            recordValues.put(attributePrefix == null ? fieldName : attributePrefix + fieldName, value);
                        }
                    } else {
                        recordValues.put(attributePrefix == null ? fieldName : attributePrefix + fieldName, attribute.getValue());
                    }
                }
            } else {
                if (coerceTypes) {
                    Optional<RecordField> field = schema.getField(fieldName);
                    if (field.isPresent()){
                        Object value;
                        if ((value = parseAttributeForType(attribute, fieldName, field.get().getDataType())) != null) {
                            recordValues.put(attributePrefix == null ? fieldName : attributePrefix + fieldName, value);
                        }
                    } else {
                        recordValues.put(attributePrefix == null ? fieldName : attributePrefix + fieldName, attribute.getValue());
                    }
                } else {
                    recordValues.put(attributePrefix == null ? fieldName : attributePrefix + fieldName, attribute.getValue());
                }
            }
        }

        // parse fields
        while(xmlEventReader.hasNext()){
            XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                StartElement startSubElement = xmlEvent.asStartElement();
                String fieldName = startSubElement.getName().toString();
                Optional<RecordField> field = schema.getField(fieldName);
                if (field.isPresent()){
                    if (coerceTypes) {
                        Object value = parseFieldForType(startSubElement, fieldName, field.get().getDataType());
                        if (value != null) {
                            recordValues.put(fieldName, value);
                        }
                    } else {
                        // parse unknown type
                    }

                } else {
                    if (dropUnknown) {
                        skipElement();
                    } else {
                        // parse unknown type
                    }
                }
            } else if (xmlEvent.isEndElement()) {
                break;
            }
        }
        return new MapRecord(schema, recordValues);
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

    private void parseFieldForUnknownType() {

    }






    @Override
    public RecordSchema getSchema() {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
