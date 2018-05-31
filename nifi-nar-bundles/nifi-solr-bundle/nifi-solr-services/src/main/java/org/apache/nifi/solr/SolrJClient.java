package org.apache.nifi.solr;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SolrJClient extends AbstractControllerService implements SolrClientService {

    static final private List<PropertyDescriptor> properties;

    private volatile SolrClient client;
    private volatile String id;
    private volatile String transitUri;

    private volatile String basicUsername = null;
    private volatile String basicPassword = null;
    private volatile boolean basicAuthEnabled = false;

    static {
        List<PropertyDescriptor> propertiesToAdd = new ArrayList<>();

        propertiesToAdd.add(SOLR_TYPE);
        propertiesToAdd.add(SOLR_LOCATION);
        propertiesToAdd.add(SSL_CONTEXT_SERVICE);

        propertiesToAdd.add(COLLECTION);
        propertiesToAdd.add(BASIC_USERNAME);
        propertiesToAdd.add(BASIC_PASSWORD);

        propertiesToAdd.add(SOLR_SOCKET_TIMEOUT);
        propertiesToAdd.add(SOLR_CONNECTION_TIMEOUT);
        propertiesToAdd.add(SOLR_MAX_CONNECTIONS);
        propertiesToAdd.add(SOLR_MAX_CONNECTIONS_PER_HOST);

        propertiesToAdd.add(ZK_CLIENT_TIMEOUT);
        propertiesToAdd.add(ZK_CONNECTION_TIMEOUT);

        properties = Collections.unmodifiableList(propertiesToAdd);
    }

    @OnEnabled
    public void setup(final ConfigurationContext context) throws InitializationException {
        this.client = createSolrClient(context);

        this.basicUsername = context.getProperty(BASIC_USERNAME).evaluateAttributeExpressions().getValue();
        this.basicPassword = context.getProperty(BASIC_PASSWORD).evaluateAttributeExpressions().getValue();
        if (!StringUtils.isBlank(basicUsername) && !StringUtils.isBlank(basicPassword)) {
            basicAuthEnabled = true;
        }


    }


    private static SolrClient createSolrClient(final ConfigurationContext context) {
        final String solrLocation = context.getProperty(SOLR_LOCATION).evaluateAttributeExpressions().getValue();

        final Integer socketTimeout = context.getProperty(SOLR_SOCKET_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer connectionTimeout = context.getProperty(SOLR_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer maxConnections = context.getProperty(SOLR_MAX_CONNECTIONS).asInteger();
        final Integer maxConnectionsPerHost = context.getProperty(SOLR_MAX_CONNECTIONS_PER_HOST).asInteger();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_SO_TIMEOUT, socketTimeout);
        params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);

        // has to happen before the client is created below so that correct configurer would be set if needed
        if (kerberosCredentialsService != null) {
            HttpClientUtil.setConfigurer(new KerberosHttpClientConfigurer());
        }

        final HttpClient httpClient = HttpClientUtil.createClient(params);

        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            final SSLSocketFactory sslSocketFactory = new SSLSocketFactory(sslContext);
            final Scheme httpsScheme = new Scheme("https", 443, sslSocketFactory);
            httpClient.getConnectionManager().getSchemeRegistry().register(httpsScheme);
        }

        if (SOLR_TYPE_STANDARD.getValue().equals(context.getProperty(SOLR_TYPE).getValue())) {
            return new HttpSolrClient(solrLocation, httpClient);
        } else {
            final String collection = context.getProperty(COLLECTION).evaluateAttributeExpressions().getValue();
            final Integer zkClientTimeout = context.getProperty(ZK_CLIENT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            final Integer zkConnectionTimeout = context.getProperty(ZK_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

            CloudSolrClient cloudSolrClient = new CloudSolrClient(solrLocation, httpClient);
            cloudSolrClient.setDefaultCollection(collection);
            cloudSolrClient.setZkClientTimeout(zkClientTimeout);
            cloudSolrClient.setZkConnectTimeout(zkConnectionTimeout);
            return cloudSolrClient;
        }
    }

    private String getFieldNameOfUniqueKey() {
        final SolrQuery solrQuery = new SolrQuery();
        try {
            solrQuery.setRequestHandler("/schema/uniquekey");
            final QueryRequest req = new QueryRequest(solrQuery);
            if (isBasicAuthEnabled()) {
                req.setBasicAuthCredentials(getUsername(), getPassword());
            }

            return(req.process(getSolrClient()).getResponse().get("uniqueKey").toString());
        } catch (SolrServerException | IOException e) {
            getLogger().error("Solr query to retrieve uniqueKey-field failed due to {}", new Object[]{solrQuery.toString(), e}, e);
            throw new ProcessException(e);
        }
    }


    @Override
    public SolrClient getSolrClient() throws IOException { return this.client; }

    @Override
    public String getTransitUrl(String index, String type) {
        return "URI";
    }

    @Override
    public String getUsername() { return this.basicUsername; }

    @Override
    public String getPassword() { return this.basicPassword; }

    @Override
    public boolean isBasicAuthEnabled() { return this.basicAuthEnabled; }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @OnDisabled
    public void onDisabled() throws IOException {

    }
}
