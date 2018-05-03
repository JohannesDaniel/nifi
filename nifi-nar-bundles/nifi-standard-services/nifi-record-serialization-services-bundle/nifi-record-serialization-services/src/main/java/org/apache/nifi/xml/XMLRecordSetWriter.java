package org.apache.nifi.xml;

import org.apache.nifi.NullSuppression;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"xml", "resultset", "writer", "serialize", "record", "recordset", "row"})
@CapabilityDescription("Writes a RecordSet to XML. The records are wrapped by a root tag.")
public class XMLRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    public static final AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Always Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will not be written out");
    public static final AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Never Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will be written out as a null value");
    public static final AllowableValue SUPPRESS_MISSING = new AllowableValue("suppress-missing", "Suppress Missing Values",
            "When a field has a value of null, it will be written out. However, if a field is defined in the schema and not present in the record, the field will not be written out.");

    public static final AllowableValue USE_PROPERTY_AS_WRAPPER = new AllowableValue("use-property-as-wrapper", "Use Property as Wrapper",
            "The value of the property \"Array Tag Name\" will be used as the tag name to wrap elements of an array. The field name of the array field will be used for the tag name " +
                    "of the elements.");
    public static final AllowableValue USE_PROPERTY_FOR_ELEMENTS = new AllowableValue("use-property-for-elements", "Use Property for Elements",
            "The value of the property \"Array Tag Name\" will be used for the tag name of the elements of an array. The field name of the array field will be used as the tag name " +
                    "to wrap elements.");
    public static final AllowableValue NO_WRAPPING = new AllowableValue("no-wrapping", "No Wrapping",
            "The elements of an array will not be wrapped");

    public static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("suppress_nulls")
            .displayName("Suppress Null Values")
            .description("Specifies how the writer should handle a null field")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS, SUPPRESS_MISSING)
            .defaultValue(NEVER_SUPPRESS.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT_XML = new PropertyDescriptor.Builder()
            .name("pretty_print_xml")
            .displayName("Pretty Print XML")
            .description("Specifies whether or not the XML should be pretty printed")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor ROOT_TAG_NAME = new PropertyDescriptor.Builder()
            .name("root_tag_name")
            .displayName("Name of Root Tag")
            .description("Specifies the name of the XML root tag wrapping the record set")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("root")
            .required(true)
            .build();

    public static final PropertyDescriptor ARRAY_WRAPPING = new PropertyDescriptor.Builder()
            .name("array_wrapping")
            .displayName("Wrap Elements of Arrays")
            .description("Specifies how the writer wraps elements of fields of type array")
            .allowableValues(USE_PROPERTY_AS_WRAPPER, USE_PROPERTY_FOR_ELEMENTS, NO_WRAPPING)
            .defaultValue(NO_WRAPPING.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor ARRAY_TAG_NAME = new PropertyDescriptor.Builder()
            .name("array_tag_name")
            .displayName("Array Tag Name")
            .description("Name of the tag used by property \"Wrap Elements of Arrays\" to write arrays")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(SUPPRESS_NULLS);
        properties.add(PRETTY_PRINT_XML);
        properties.add(ROOT_TAG_NAME);
        properties.add(ARRAY_WRAPPING);
        properties.add(ARRAY_TAG_NAME);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        if (!getConfigurationContext().getProperty(ARRAY_WRAPPING).equals(NO_WRAPPING)) {
            if (!getConfigurationContext().getProperty(ARRAY_TAG_NAME).isSet()) {
                StringBuilder explanation = new StringBuilder()
                        .append("If property ")
                        .append(ARRAY_WRAPPING.getName())
                        .append(" is defined as ")
                        .append(USE_PROPERTY_AS_WRAPPER.getDisplayName())
                        .append(" or ")
                        .append(USE_PROPERTY_FOR_ELEMENTS.getDisplayName())
                        .append(" property ")
                        .append(ARRAY_TAG_NAME.getDisplayName())
                        .append(" has to be set.");

                return Collections.singleton(new ValidationResult.Builder()
                        .subject(ARRAY_TAG_NAME.getName())
                        .valid(false)
                        .explanation(explanation.toString())
                        .build());
            }
        }
        return Collections.emptyList();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) throws SchemaNotFoundException, IOException {

        final PropertyValue nullSuppression = getConfigurationContext().getProperty(SUPPRESS_NULLS);
        final NullSuppression nullSuppressionEnum;
        if (nullSuppression.equals(ALWAYS_SUPPRESS)) {
            nullSuppressionEnum = NullSuppression.ALWAYS_SUPPRESS;
        } else if (nullSuppression.equals(NEVER_SUPPRESS)) {
            nullSuppressionEnum = NullSuppression.NEVER_SUPPRESS;
        } else {
            nullSuppressionEnum = NullSuppression.SUPPRESS_MISSING;
        }

        final boolean prettyPrint = getConfigurationContext().getProperty(PRETTY_PRINT_XML).equals("true");

        final String rootTagName = getConfigurationContext().getProperty(ROOT_TAG_NAME).getValue();

        final PropertyValue arrayWrapping = getConfigurationContext().getProperty(ARRAY_WRAPPING);
        final ArrayWrapping arrayWrappingEnum;
        if (arrayWrapping.equals(NO_WRAPPING)) {
            arrayWrappingEnum = ArrayWrapping.NO_WRAPPING;
        } else if (arrayWrapping.equals(USE_PROPERTY_AS_WRAPPER)) {
            arrayWrappingEnum = ArrayWrapping.USE_PROPERTY_AS_WRAPPER;
        } else {
            arrayWrappingEnum = ArrayWrapping.USE_PROPERTY_FOR_ELEMENTS;
        }

        final String arrayTagName;
        if (getConfigurationContext().getProperty(ARRAY_TAG_NAME).isSet()) {
            arrayTagName = getConfigurationContext().getProperty(ARRAY_TAG_NAME).getValue();
        } else {
            arrayTagName = null;
        }

        return new WriteXMLResult(logger, schema, getSchemaAccessWriter(schema),
                out, prettyPrint, nullSuppressionEnum, arrayWrappingEnum, arrayTagName, rootTagName,
                getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null));
    }
}
