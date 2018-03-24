/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.xml;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XMLReader extends SchemaRegistryService implements RecordReaderFactory {

    public static final PropertyDescriptor VALIDATE_ROOT_TAG = new PropertyDescriptor.Builder()
            .name("validate_root_tag")
            .displayName("validate_root_tag")
            .description("validate_root_tag")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor VALIDATE_RECORD_TAG = new PropertyDescriptor.Builder()
            .name("validate_record_tag")
            .displayName("validate_record_tag")
            .description("validate_record_tag")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name("attribute_prefix")
            .displayName("attribute_prefix")
            .description("attribute_prefix")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor CONTENT_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("content_field_name")
            .displayName("content_field_name")
            .description("content_field_name")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
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
        final String rootName = context.getProperty(VALIDATE_ROOT_TAG).isSet() ? context.getProperty(VALIDATE_ROOT_TAG).getValue().trim() : null;
        final String recordName = context.getProperty(VALIDATE_RECORD_TAG).isSet() ? context.getProperty(VALIDATE_RECORD_TAG).getValue().trim() : null;
        final String attributePrefix = context.getProperty(ATTRIBUTE_PREFIX).isSet() ? context.getProperty(ATTRIBUTE_PREFIX).getValue().trim() : null;
        final String contentFieldName = context.getProperty(CONTENT_FIELD_NAME).isSet() ? context.getProperty(CONTENT_FIELD_NAME).getValue().trim() : null;

        return new XMLRecordReader(in, schema, rootName, recordName, attributePrefix, contentFieldName, dateFormat, timeFormat, timestampFormat, logger);
    }
}
