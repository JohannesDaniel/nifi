package org.apache.nifi.xml;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.nifi.serialization.record.RecordFieldType.ARRAY;

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

    public XMLRecordReader(InputStream in, RecordSchema schema, boolean isArray, String recordName, String attributePrefix,
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

    private Object parseFieldForType(StartElement startElement, String fieldName, DataType dataType, Map<String, Object> recordValues) throws XMLStreamException {
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
                Characters characters = xmlEventReader.nextEvent().asCharacters();
                xmlEventReader.nextEvent();
                return DataTypeUtils.convertType(characters.toString(), dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
            }
            case ARRAY: {
                final DataType arrayDataType = ((ArrayDataType) dataType).getElementType();

                final Object newValue = parseFieldForType(startElement, fieldName, arrayDataType, recordValues);
                final Object oldValues = recordValues.get(fieldName);

                if (oldValues != null) {
                    if (oldValues instanceof List) {
                        ((List) oldValues).add(newValue);
                    } else {
                        return new ArrayList<Object>(){{ add(oldValues); add(newValue); }};
                    }
                } else {
                    return new ArrayList<Object>(){{ add(newValue); }};
                }
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
                // skip element?
                return null;
            }
            case CHOICE: {
                // Type cannot be determined a priori
                return null;
            }
        }
        return null;
    }

    private Object parseUnknownField(StartElement startElement) throws XMLStreamException {
        final String fieldName = startElement.getName().toString();

        // attributes
        Map<String, Object> recordValues = new HashMap<>();
        Iterator iterator = startElement.getAttributes();
        while (iterator.hasNext()) {
            Attribute attribute = (Attribute) iterator.next();
            final String attributeName = attribute.getName().toString();
            recordValues.put(attributePrefix == null ? attributeName : attributePrefix + attributeName, attribute.getValue());
        }
        boolean hasAttributes = recordValues.size() > 0;

        while (xmlEventReader.hasNext()) {
            XMLEvent xmlEvent = xmlEventReader.nextEvent();
            if (xmlEvent.isCharacters()) {
                Characters characters = xmlEvent.asCharacters();
                if (!characters.isWhiteSpace()) {
                    xmlEventReader.nextEvent();
                    if (hasAttributes) {
                        recordValues.put(fieldName, characters.toString());
                        xmlEventReader.nextEvent();
                        return new MapRecord(new SimpleRecordSchema(Collections.emptyList()), recordValues);
                    } else {
                        return characters.toString();
                    }
                }
            } else if (xmlEvent.isStartElement()){
                final StartElement subStartElement = xmlEvent.asStartElement();
                final String subFieldName = subStartElement.getName().toString();

                putUnknownTypeInMap(recordValues, subFieldName, parseUnknownField(subStartElement));

            } else if (xmlEvent.isEndElement()) {
                break;
            }
        }

        for (Map.Entry<String,Object> entry : recordValues.entrySet()) {
            if (entry.getValue() instanceof List) {
                recordValues.put(entry.getKey(), ((List) entry.getValue()).toArray());
            }
        }

        return new MapRecord(new SimpleRecordSchema(Collections.emptyList()), recordValues);
    }

    private Record parseRecord(StartElement startElement, RecordSchema schema, boolean coerceTypes, boolean dropUnknown) throws XMLStreamException {
        Map<String, Object> recordValues = new HashMap<>();

        // parse attributes
        Iterator iterator = startElement.getAttributes();
        while (iterator.hasNext()) {
            Attribute attribute = (Attribute) iterator.next();
            String attributeName = attribute.getName().toString();

            // Notice that if attributePrefix is set the reader will create a field that has no corresponding schema entry
            String targetFieldName = attributePrefix == null ? attributeName : attributePrefix + attributeName;

            if (dropUnknown) {
                Optional<RecordField> field = schema.getField(attributeName);
                if (field.isPresent()){

                    // dropUnknown == true && coerceTypes == true
                    if (coerceTypes) {
                        Object value;
                        DataType dataType = field.get().getDataType();
                        if ((value = parseAttributeForType(attribute, attributeName, dataType)) != null) {
                            recordValues.put(targetFieldName, value);
                        }

                    // dropUnknown == true && coerceTypes == false
                    } else {
                        recordValues.put(targetFieldName, attribute.getValue());
                    }
                }
            } else {

                // dropUnknown == false && coerceTypes == true
                if (coerceTypes) {
                    Object value;
                    Optional<RecordField> field = schema.getField(attributeName);
                    if (field.isPresent()){
                        if ((value = parseAttributeForType(attribute, attributeName, field.get().getDataType())) != null) {
                            recordValues.put(targetFieldName, value);
                        }
                    } else {
                        recordValues.put(targetFieldName, attribute.getValue());
                    }

                    // dropUnknown == false && coerceTypes == false
                } else {
                    recordValues.put(targetFieldName, attribute.getValue());
                }
            }
        }

        // parse fields
        while(xmlEventReader.hasNext()){
            XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                StartElement subStartElement = xmlEvent.asStartElement();
                String fieldName = subStartElement.getName().toString();
                Optional<RecordField> field = schema.getField(fieldName);

                if (dropUnknown) {
                    if (field.isPresent()) {
                        // dropUnknown == true && coerceTypes == true
                        if (coerceTypes) {
                            Object value = parseFieldForType(subStartElement, fieldName, field.get().getDataType(), recordValues);
                            if (value != null) {
                                recordValues.put(fieldName, value);
                            }

                        // dropUnknown == true && coerceTypes == false
                        } else {
                            // parse unknown type
                        }

                    } else {
                        skipElement();
                    }
                } else {
                    // dropUnknown == false && coerceTypes == true
                    if (coerceTypes) {
                        if (field.isPresent()) {
                            Object value = parseFieldForType(subStartElement, fieldName, field.get().getDataType(), recordValues);
                            if (value != null) {
                                recordValues.put(fieldName, value);
                            }
                        } else {
                            // parse unknown type
                        }

                    } else {
                        // parse unknown type
                        putUnknownTypeInMap(recordValues, fieldName, parseUnknownField(subStartElement));
                        //recordValues.put(fieldName, parseUnknownField(subStartElement));
                    }
                }
            } else if (xmlEvent.isEndElement()) {
                break;
            }
        }
        for (Map.Entry<String,Object> entry : recordValues.entrySet()) {
            if (entry.getValue() instanceof List) {
                recordValues.put(entry.getKey(), ((List) entry.getValue()).toArray());
            }
        }

        return new MapRecord(schema, recordValues);
    }

    private void putUnknownTypeInMap(Map<String, Object> values, String fieldName, Object fieldValue) {
        final Object oldValues = values.get(fieldName);

        if (oldValues != null) {
            if (oldValues instanceof List) {
                ((List) oldValues).add(fieldValue);
            } else {
                values.put(fieldName, new ArrayList<Object>(){{ add(oldValues); add(fieldValue); }});
            }
        } else {
            values.put(fieldName, fieldValue);
        }
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

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() throws IOException {
    }
}
