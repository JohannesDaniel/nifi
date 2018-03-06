package org.apache.nifi.xml;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XMLReader extends SchemaRegistryService implements RecordReaderFactory {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        // Use Mark/Reset of a BufferedInputStream in case we read from the Input Stream for the header.
        /*
        final BufferedInputStream bufferedIn = new BufferedInputStream(in);
        bufferedIn.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(bufferedIn), null);
        bufferedIn.reset();

        if(APACHE_COMMONS_CSV.getValue().equals(csvParser)) {
            return new CSVRecordReader(bufferedIn, logger, schema, csvFormat, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet);
        } else if(JACKSON_CSV.getValue().equals(csvParser)) {
            return new JacksonCSVRecordReader(bufferedIn, logger, schema, csvFormat, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet);
        } else {
            throw new IOException("Parser not supported");
        }
        */
        return new XMLRecordReader();
    }
}
