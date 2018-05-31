package org.apache.nifi.solr;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SolrJClient extends AbstractControllerService implements SolrClientService {

    static final private List<PropertyDescriptor> properties;

    private SolrClient client;

    private String url;
    private Charset charset;

    static {
        List<PropertyDescriptor> _props = new ArrayList<>();

        _props.add(SOLR_TYPE);
        _props.add(SOLR_LOCATION);
        _props.add(SSL_CONTEXT_SERVICE);

        // how to include?
        _props.add(COLLECTION);
        _props.add(BASIC_USERNAME);
        _props.add(BASIC_PASSWORD);

        _props.add(SOLR_SOCKET_TIMEOUT);
        _props.add(SOLR_CONNECTION_TIMEOUT);
        _props.add(SOLR_MAX_CONNECTIONS);
        _props.add(SOLR_MAX_CONNECTIONS_PER_HOST);

        // not used
        _props.add(ZK_CLIENT_TIMEOUT);
        _props.add(ZK_CONNECTION_TIMEOUT);

        properties = Collections.unmodifiableList(_props);
    }


    private SolrClient createClient(ConfigurationContext context) {

        final Integer socketTimeout = context.getProperty(SOLR_SOCKET_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer connectionTimeout = context.getProperty(SOLR_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer maxConnections = context.getProperty(SOLR_MAX_CONNECTIONS).asInteger();
        final Integer maxConnectionsPerHost = context.getProperty(SOLR_MAX_CONNECTIONS_PER_HOST).asInteger();
        
        final boolean isCloudMode = context.getProperty(SOLR_TYPE).getValue().equals(SOLR_TYPE_CLOUD.getValue());

        final String solrLocation = context.getProperty(SOLR_LOCATION).evaluateAttributeExpressions().getValue();
        
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        final PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();
        //poolingHttpClientConnectionManager.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(socketTimeout).build());
        //poolingHttpClientConnectionManager.setDefaultConnectionConfig(ConnectionConfig.custom().);
        poolingHttpClientConnectionManager.setMaxTotal(maxConnections);
        poolingHttpClientConnectionManager.setDefaultMaxPerRoute(maxConnectionsPerHost);

        final RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout).build();

        final HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .setConnectionManager(poolingHttpClientConnectionManager)
                .setDefaultRequestConfig(requestConfig);

        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            httpClientBuilder.setSslcontext(sslContext);
        }

        // Kerberos

        final HttpClient httpClient = httpClientBuilder.build();

        SolrClient solrClient;
        if (isCloudMode) {
            solrClient = new CloudSolrClient.Builder().withZkHost(solrLocation).withHttpClient(httpClient).build();
        } else {
            solrClient = new HttpSolrClient.Builder(solrLocation).withHttpClient(httpClient).build();
        }


        return solrClient;
    }
    
    @Override
    public String search(String query, String index, String type) throws IOException {
        //

        return "SEARCH";
    }

    @Override
    public String getTransitUrl(String index, String type) {
        return "URI";
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

    }

    @OnDisabled
    public void onDisabled() throws IOException {

    }
}
