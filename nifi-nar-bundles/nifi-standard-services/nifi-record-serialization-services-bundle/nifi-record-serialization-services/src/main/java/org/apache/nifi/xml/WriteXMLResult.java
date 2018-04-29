package org.apache.nifi.xml;

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;


public class WriteXMLResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {

    final ComponentLog logger;
    final RecordSchema recordSchema;
    final SchemaAccessWriter schemaAccess;
    final XMLStreamWriter writer;
    final NullSuppression nullSuppression;
    final ArrayWrapping arrayWrapping;
    final String arrayWrappingValue;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    public WriteXMLResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
                          final NullSuppression nullSuppression, final ArrayWrapping arrayWrapping, final String arrayWrappingValue,
                          final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {

        super(out);

        this.logger = logger;
        this.recordSchema = recordSchema;
        this.schemaAccess = schemaAccess;
        this.nullSuppression = nullSuppression;
        this.arrayWrapping = arrayWrapping;
        this.arrayWrappingValue = arrayWrappingValue;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        try {
            XMLOutputFactory factory = XMLOutputFactory.newInstance();

            // should I consider the encoding somehow (e. g. via property)?
            // writer = factory.createXMLStreamWriter(out, StandardCharsets.UTF_8.name());

            if (prettyPrint) {
                writer = new IndentingXMLStreamWriter(factory.createXMLStreamWriter(out));
            } else {
                writer = factory.createXMLStreamWriter(out);
            }

            /*
            XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = new IndentingXMLStreamWriter(xmlof.createXMLStreamWriter(out));
             */

        } catch (XMLStreamException e) {
            throw new IOException(e.getMessage());
        }

    }

    @Override
    protected void onBeginRecordSet() throws IOException {

        final OutputStream out = getOutputStream();
        schemaAccess.writeHeader(recordSchema, out);

        try {
            writer.writeStartDocument();

            writer.writeStartElement("ROOT");

        } catch (XMLStreamException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {

        try {
            writer.writeEndElement();
            writer.writeEndDocument();
        } catch (XMLStreamException e) {
            throw new IOException(e.getMessage());
        }
        return schemaAccess.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {

        try {
            writer.close();

        } catch (XMLStreamException e) {
            throw new IOException(e.getMessage());
        }

        super.close();
    }

    @Override
    public void flush() throws IOException {

        try {
            writer.flush();

        } catch (XMLStreamException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    protected Map<String, String> writeRecord(Record record) throws IOException {

        if (!isActiveRecordSet()) {
            // flush of writer ??
            schemaAccess.writeHeader(recordSchema, getOutputStream());
        }
        // coerce == true
        System.out.println(record);

        List<String> tagsToOpen = new ArrayList<>();

        try {
            Optional<String> recordNameOptional = recordSchema.getIdentifier().getName();
            if (recordNameOptional.isPresent()) {
                tagsToOpen.add(recordNameOptional.get());
            } else {
                throw new IOException("No name for record schema available");
            }

            boolean closingTagRequired = writeRecordUsingSchema(tagsToOpen, record, recordSchema);
            if (closingTagRequired) {
                writer.writeEndElement();
            }

        } catch (XMLStreamException e) {
            throw new IOException(e.getMessage());
        }
        return schemaAccess.getAttributes(recordSchema);
    }

    private boolean writeRecordUsingSchema(List<String> tagsToOpen, Record record, RecordSchema schema) throws XMLStreamException {

        // "global"
        boolean requireGlobalEndTag = false;
        for (RecordField field : schema.getFields()) {

            String fieldName = field.getFieldName();
            DataType dataType = field.getDataType();
            Object value = record.getValue(field);

            boolean requireEndTag = false;

            final Object coercedValue = DataTypeUtils.convertType(value, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);

            if (coercedValue != null) {
                tagsToOpen.add(fieldName);
                requireEndTag = writeField(tagsToOpen, coercedValue, dataType, schema);

                if (!requireEndTag) {
                    tagsToOpen.remove(tagsToOpen.size() - 1);
                }

            } else {
                if (nullSuppression.equals(NullSuppression.NEVER_SUPPRESS) || nullSuppression.equals(NullSuppression.SUPPRESS_MISSING) && recordHasField(field, record)) {
                    tagsToOpen.add(fieldName);
                    writeAllTags(tagsToOpen);
                    requireEndTag = true;
                }
            }

            if (requireEndTag) {
                requireGlobalEndTag = true;
                writer.writeEndElement();
            }
        }

        return requireGlobalEndTag;
    }

    private boolean writeField(List<String> tagsToOpen, Object coercedValue, DataType dataType, RecordSchema schema) throws XMLStreamException {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING: {
                writeAllTags(tagsToOpen);
                writer.writeCharacters(coercedValue.toString());
                return true;
            }
            case DATE: {
                writeAllTags(tagsToOpen);
                final String stringValue = DataTypeUtils.toString(coercedValue, LAZY_DATE_FORMAT);
                writer.writeCharacters(stringValue);
                return true;
            }
            case TIME: {
                writeAllTags(tagsToOpen);
                final String stringValue = DataTypeUtils.toString(coercedValue, LAZY_TIME_FORMAT);
                writer.writeCharacters(stringValue);
                return true;
            }
            case TIMESTAMP: {
                writeAllTags(tagsToOpen);
                final String stringValue = DataTypeUtils.toString(coercedValue, LAZY_TIMESTAMP_FORMAT);
                writer.writeCharacters(stringValue);
                return true;
            }
            case RECORD: {
                final Record record = (Record) coercedValue;
                final RecordDataType recordDataType = (RecordDataType) dataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();
                return writeRecordUsingSchema(tagsToOpen, record, childSchema);
            }
            case ARRAY: {
                if (arrayWrapping.equals(ArrayWrapping.NO_WRAPPING)) {
                    if (coercedValue instanceof Object[]) {
                        final Object[] arrayValues = (Object[]) coercedValue;
                        final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                        final DataType elementType = arrayDataType.getElementType();
                        //writeArray(values, fieldName, generator, elementType);
                    } else {
                       // generator.writeString(coercedValue.toString());
                    }
                }
            }

            // choice
            // map
            // array

        }


        /*
        case DATE: {
                final String stringValue = DataTypeUtils.toString(coercedValue, LAZY_DATE_FORMAT);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
         */
        return false;
    }

    private void writeAllTags(List<String> tagsToOpen) throws XMLStreamException {
        for (String tagName : tagsToOpen) {
            writer.writeStartElement(tagName);
        }
        tagsToOpen.clear();
    }

    @Override
    public WriteResult writeRawRecord(Record record) throws IOException {
        // coerce == false

        return null;
    }

    @Override
    public String getMimeType() {
        return null;
    }

    private boolean recordHasField(RecordField field, Record record) {
        Set<String> recordFieldNames = record.getRawFieldNames();
        if (recordFieldNames.contains(field.getFieldName())) {
            return true;
        }

        for (String alias : field.getAliases()) {
            if (recordFieldNames.contains(alias)) {
                return true;
            }
        }

        return false;
    }
}
