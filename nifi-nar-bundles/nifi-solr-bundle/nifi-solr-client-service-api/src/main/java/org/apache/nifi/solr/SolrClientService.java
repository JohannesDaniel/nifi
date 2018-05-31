package org.apache.nifi.solr;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.solr.client.solrj.SolrClient;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public interface SolrClientService extends ControllerService {

    // change descriptions

    AllowableValue SOLR_TYPE_CLOUD = new AllowableValue(
            "Cloud", "Cloud", "A SolrCloud instance.");

    AllowableValue SOLR_TYPE_STANDARD = new AllowableValue(
            "Standard", "Standard", "A stand-alone Solr instance.");

    PropertyDescriptor SOLR_TYPE = new PropertyDescriptor
            .Builder().name("Solr Type")
            .description("The type of Solr instance, Cloud or Standard.")
            .required(true)
            .allowableValues(SOLR_TYPE_CLOUD, SOLR_TYPE_STANDARD)
            .defaultValue(SOLR_TYPE_STANDARD.getValue())
            .build();

    PropertyDescriptor COLLECTION = new PropertyDescriptor
            .Builder().name("Collection")
            .description("The Solr collection name, only used with a Solr Type of Cloud")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    PropertyDescriptor SOLR_LOCATION = new PropertyDescriptor
            .Builder().name("Solr Location")
            .description("The Solr url for a Solr Type of Standard (ex: http://localhost:8984/solr/gettingstarted), " +
                    "or the ZooKeeper hosts for a Solr Type of Cloud (ex: localhost:9983).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    PropertyDescriptor BASIC_USERNAME = new PropertyDescriptor
            .Builder().name("Username")
            .description("The username to use when Solr is configured with basic authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    PropertyDescriptor BASIC_PASSWORD = new PropertyDescriptor
            .Builder().name("Password")
            .description("The password to use when Solr is configured with basic authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .build();

    PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. This property must be set when communicating with a Solr over https.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();


    PropertyDescriptor SOLR_SOCKET_TIMEOUT = new PropertyDescriptor
            .Builder().name("Solr Socket Timeout")
            .description("The amount of time to wait for data on a socket connection to Solr. A value of 0 indicates an infinite timeout.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .build();

    PropertyDescriptor SOLR_CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder().name("Solr Connection Timeout")
            .description("The amount of time to wait when establishing a connection to Solr. A value of 0 indicates an infinite timeout.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .build();

    PropertyDescriptor SOLR_MAX_CONNECTIONS = new PropertyDescriptor
            .Builder().name("Solr Maximum Connections")
            .description("The maximum number of total connections allowed from the Solr client to Solr.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    PropertyDescriptor SOLR_MAX_CONNECTIONS_PER_HOST = new PropertyDescriptor
            .Builder().name("Solr Maximum Connections Per Host")
            .description("The maximum number of connections allowed from the Solr client to a single Solr host.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    PropertyDescriptor ZK_CLIENT_TIMEOUT = new PropertyDescriptor
            .Builder().name("ZooKeeper Client Timeout")
            .description("The amount of time to wait for data on a connection to ZooKeeper, only used with a Solr Type of Cloud.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("10 seconds")
            .build();

    PropertyDescriptor ZK_CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder().name("ZooKeeper Connection Timeout")
            .description("The amount of time to wait when establishing a connection to ZooKeeper, only used with a Solr Type of Cloud.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("10 seconds")
            .build();


    /**
     * Perform a getSolrClient using the JSON DSL.
     * @return A SearchResponse object if successful.
     */
    SolrClient getSolrClient() throws IOException;

    /**
     * Build a transit URL to use with the provenance reporter.
     * @param index Index targeted. Optional.
     * @param type Type targeted. Optional
     * @return a URL describing the ElasticSearch cluster.
     */
    String getTransitUrl(String index, String type);

    String getUsername();

    String getPassword();

    boolean isBasicAuthEnabled();

}
