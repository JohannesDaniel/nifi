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

    public static final PropertyDescriptor VALIDATE_ROOT_TAG = new PropertyDescriptor.Builder()
            .name("validate_root_tag")
            .displayName("")
            .description("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    public static final PropertyDescriptor VALIDATE_RECORD_TAG = new PropertyDescriptor.Builder()
            .name("validate_record_tag")
            .displayName("")
            .description("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name("attribute_prefix")
            .displayName("")
            .description("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(VALIDATE_ROOT_TAG);
        properties.add(VALIDATE_RECORD_TAG);
        properties.add(ATTRIBUTE_PREFIX);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, SchemaNotFoundException, MalformedRecordException {
        final ConfigurationContext context = getConfigurationContext();

        final RecordSchema schema = getSchema(variables, in, null);
        final String rootName = context.getProperty(VALIDATE_ROOT_TAG).isSet() ? context.getProperty(VALIDATE_ROOT_TAG).getValue() : null;
        final String recordName = context.getProperty(VALIDATE_RECORD_TAG).isSet() ? context.getProperty(VALIDATE_RECORD_TAG).getValue() : null;
        final String attributePrefix = context.getProperty(ATTRIBUTE_PREFIX).isSet() ? context.getProperty(ATTRIBUTE_PREFIX).getValue() : null;

        return new XMLRecordReader(in, schema, rootName, recordName, attributePrefix, dateFormat, timeFormat, timestampFormat, logger);
    }
}
