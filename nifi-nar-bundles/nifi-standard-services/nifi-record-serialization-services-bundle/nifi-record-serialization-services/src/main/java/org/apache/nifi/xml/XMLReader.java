package org.apache.nifi.xml;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class XMLReader extends SchemaRegistryService implements RecordReaderFactory {

    /*
    Properties
        - Record Path
        - Content field
        - Attribute Prefix
     */
    public static final AllowableValue EXPECT_SINGLE_RECORD = new AllowableValue("expect_single_record", "Expect single record");
    public static final AllowableValue EXPECT_ARRAY_OF_RECORDS = new AllowableValue("expect_array_of_records", "Expect an array of records");

    // describe usage /root or /root/record
    public static final PropertyDescriptor VALIDATE_XML_PATH_TO_RECORD = new PropertyDescriptor.Builder()
            .name("validate_xml_path_to_record")
            .displayName("")
            .description("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    public static final PropertyDescriptor EXPECTED_DATA = new PropertyDescriptor.Builder()
            .name("expected_data")
            .displayName("Define whether the reader shall expect a single record or an array of records.")
            .description("")
            .allowableValues(EXPECT_SINGLE_RECORD, EXPECT_ARRAY_OF_RECORDS)
            .defaultValue(EXPECT_ARRAY_OF_RECORDS.getValue())
            .required(true)
            .build();

//    private volatile List<String> xmlRecordPathElements = null;
    private volatile Tuple<String, String> xmlRecordPathElements = null;

    // ggf. XMLPathValidator (wäre cool)
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // Record Path

      //  if (descriptor.equals(XML_PATH_TO_RECORD))
        //    xmlRecordPathElements = Collections.unmodifiableList(Arrays.asList(newValue.trim().split("/")));
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Collection<ValidationResult> problems = new ArrayList<>();


        if (validationContext.getProperty(VALIDATE_XML_PATH_TO_RECORD).isSet()) {
            final List<String> elementsList = Arrays.asList(
                    validationContext.getProperty(VALIDATE_XML_PATH_TO_RECORD).getValue().trim().split("/"));

            switch (elementsList.size()) {
                case 1: xmlRecordPathElements = new Tuple<>(elementsList.get(0), null); break;
                case 2: xmlRecordPathElements = new Tuple<>(elementsList.get(0), elementsList.get(1)); break;
                // ValidationResult adden
                default: problems.add(null);
            }
        }

        return problems;
    }



        @OnEnabled
    public void parseXmlPath(final ConfigurationContext context) {

    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, SchemaNotFoundException, MalformedRecordException {
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

        final RecordSchema schema = getSchema(variables, in, null);
        return new XMLRecordReader(in, schema, xmlRecordPathElements);
    }
}
