package pl.kwitukiewicz.wdb.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author Krzysztof Witukiewicz
 */
public abstract class ElasticsearchRepository {
    protected final String index;
    // Retained for API compatibility: ES 8 removed mapping types, so this is no longer
    // used for any request. The constructor signature is kept so subclasses are unchanged.
    protected final String type;

    protected final RestClient lowLevelClient;
    protected final ElasticsearchClient client;

    protected ElasticsearchRepository(String index, String type) {
        Validate.notBlank(index);
        Validate.notBlank(type);
        this.index = index;
        this.type = type;
        lowLevelClient = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                                   .setHttpClientConfigCallback(httpClientBuilder -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                                               new UsernamePasswordCredentials("elastic",
                                                                               "changeme"));
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        })
                                   .build();
        client = new ElasticsearchClient(new RestClientTransport(lowLevelClient, new JacksonJsonpMapper()));
    }

    public void cleanup() throws IOException {
        lowLevelClient.close();
    }

    protected abstract Logger getLogger();
}
