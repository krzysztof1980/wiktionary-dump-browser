package pl.kwitukiewicz.wdb.elasticsearch

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpEntity
import org.apache.http.HttpHost
import org.apache.http.HttpStatus
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Krzysztof Witukiewicz
 */
class CreateIndexService {

    public static final String INDEX = "dewiktionary_dump"
    public static final String TYPE = "page"

    private static final Logger logger = LoggerFactory.getLogger(CreateIndexService.class)
    private static final Logger errorLogger = LoggerFactory.getLogger("indexation_errors")

    private final RestClient client
    private final ObjectMapper objectMapper

    private final AtomicInteger indexationCounter = new AtomicInteger()
    private final Map<String, Exception> indexationExceptions = new ConcurrentHashMap<>()

    private CreateIndexService() {
        client = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                           .setHttpClientConfigCallback({httpClientBuilder ->
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider()
            credentialsProvider.setCredentials(AuthScope.ANY,
                                               new UsernamePasswordCredentials("elastic",
                                                                               "changeme"))
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        })
                           .build()
        objectMapper = new ObjectMapper()
    }

    static CreateIndexService create() {
        CreateIndexService service = new CreateIndexService()
        service.initialize()
        return service
    }

    void indexWiktionaryPage(WiktionaryPageDocument document) {
        String id
        try {
            id = URLEncoder.encode(document.getNamespace() + ":" + document.getTitle(), "UTF-8")
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Error indexing " + document, e)
        }
        String endpoint = "/${INDEX}/${TYPE}/${id}"
        byte[] body
        try {
            body = objectMapper.writeValueAsBytes(document)
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e)
        }
        ByteArrayEntity entity = new ByteArrayEntity(body)
        try {
            client.performRequest("PUT", endpoint, Collections.emptyMap(), entity,
                                  new BasicHeader("Content-Type", ContentType.APPLICATION_JSON.toString()))
            indexationCounter.incrementAndGet()
        } catch (Exception e) {
            indexationExceptions.put(id, e)
        }
        def count = indexationCounter.get() + indexationExceptions.size()
        if (count % 1000 == 0) {
            logger.info("Indexed ${count} documents so far!")
        }
    }

    void cleanup() {
        resetRefreshInterval()
        logger.info("The indexation was finished! Successfully indexed {} documents. There were {} errors!",
                    indexationCounter.get(), indexationExceptions.size())
        if (!indexationExceptions.isEmpty()) {
            errorLogger.error("There were {} errors during indexation! The errors are:", indexationExceptions.size())
            indexationExceptions.forEach({id, e -> errorLogger.error("Exception indexing document id=${id}", e)})
        }
        try {
            client.close()
        } catch (IOException e) {
            logger.error("Error closing client!", e)
        }
    }

    private void initialize() {
        deleteIndex()
        createIndex()
    }

    private void deleteIndex() {
        try {
            client.performRequest("DELETE", "/" + INDEX)
            logger.info("Deleted index ${INDEX}")
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                logger.info("Index {} does not exist - nothing to delete.", INDEX)
            } else {
                throw new RuntimeException("Unexpected error deleting index " + INDEX, e)
            }
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error deleting index " + INDEX, e)
        }
    }

    private void createIndex() {
        def jsonString = """\
{
    "settings": {
        "index": {
            "number_of_shards": "1",
            "number_of_replicas": "0",
            "refresh_interval": "-1"
        }
    },
    "mappings": {
        "${TYPE}": {
            "properties": {
                "namespace": {
                    "type": "text"
                },
                "title": {
                    "type": "text",
                    "fields": {
                        "exact": {
                            "type": "keyword"
                        }
                    }
                },
                "text": {
                    "type": "text",
                    "index": false
                }
            }
        }
    }
}"""
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON)
        try {
            client.performRequest("PUT", INDEX, Collections.emptyMap(), entity)
            logger.info("Created index ${INDEX}")
        } catch (Exception e) {
            throw new RuntimeException("Error creating index " + INDEX, e)
        }
    }

    private void resetRefreshInterval() {
        def jsonString = """\
{
    "settings": {
        "index": {
            "refresh_interval": "1s"
        }
    }
}"""
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON)
        try {
            client.performRequest("PUT", INDEX + "/_settings", Collections.emptyMap(), entity)
        } catch (Exception e) {
            logger.error("Error setting refresh_interval to default value", e)
        }
    }
}
