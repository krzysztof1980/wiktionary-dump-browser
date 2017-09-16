package pl.kwitukiewicz.wdb.elasticsearch

import org.apache.commons.lang3.Validate
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.slf4j.Logger

/**
 * @author Krzysztof Witukiewicz
 */
abstract class ElasticsearchRepository {
    protected final String index
    protected final String type

    protected final RestClient lowLevelClient
    protected final RestHighLevelClient client

    protected ElasticsearchRepository(String index, String type) {
        Validate.notBlank(index)
        Validate.notBlank(type)
        this.index = index
        this.type = type
        lowLevelClient = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                                   .setHttpClientConfigCallback({ httpClientBuilder ->
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider()
            credentialsProvider.setCredentials(AuthScope.ANY,
                                               new UsernamePasswordCredentials("elastic",
                                                                               "changeme"))
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        })
                                   .build()
        client = new RestHighLevelClient(lowLevelClient)
    }

    void cleanup() {
        lowLevelClient.close()
    }

    protected abstract Logger getLogger()
}
