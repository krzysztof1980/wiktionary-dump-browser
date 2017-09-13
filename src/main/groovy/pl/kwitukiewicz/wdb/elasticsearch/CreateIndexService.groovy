package pl.kwitukiewicz.wdb.elasticsearch

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpEntity
import org.apache.http.HttpHost
import org.apache.http.HttpStatus
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.ResponseException
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.ByteSizeUnit
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.threadpool.ThreadPool
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Krzysztof Witukiewicz
 */
class CreateIndexService {

    public static final String INDEX = "dewiktionary_dump"
    public static final String TYPE = "page"

    private static final Logger logger = LoggerFactory.getLogger(CreateIndexService.class)
    private static final Logger errorLogger = LoggerFactory.getLogger("indexation_errors")

    private final RestClient lowLevelClient
    private final RestHighLevelClient client
    private final ThreadPool threadPool
    private final BulkProcessor bulkProcessor
    private final ObjectMapper objectMapper

    private final AtomicInteger indexingCounter = new AtomicInteger()

    private final BulkProcessor.Listener bulkProcessorListener = new BulkProcessor.Listener() {

        @Override
        void beforeBulk(long executionId, BulkRequest request) {
            // do nothing
        }

        @Override
        void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                errorLogger.error(response.buildFailureMessage())
            } else {
                int currCount = indexingCounter.addAndGet(request.numberOfActions())
                if (currCount % 10000 == 0) {
                    logger.info("Indexed ${currCount} documents so far!")
                }
            }
        }

        @Override
        void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            errorLogger.error("Exception was thrown during bulk execution " + executionId, failure)
        }
    }

    private CreateIndexService() {
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
        threadPool = new ThreadPool(Settings.builder().put("node.name", "dummy").build())
        bulkProcessor = new BulkProcessor.Builder({ r, l -> client.bulkAsync(r, l) },
                                                  bulkProcessorListener,
                                                  threadPool)
                .setBulkSize(new ByteSizeValue(15, ByteSizeUnit.MB))
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
        byte[] body
        try {
            body = objectMapper.writeValueAsBytes(document)
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e)
        }
        bulkProcessor.add(
                new IndexRequest(INDEX, TYPE, id)
                        .opType(DocWriteRequest.OpType.CREATE)
                        .source(body, XContentType.JSON))
    }

    void cleanup() {
        resetRefreshInterval()
        logger.info("Cleaning up after indexing...")
        try {
            if (bulkProcessor.awaitClose(5, TimeUnit.MINUTES)) {
                logger.info("All bulk requests finished.")
            } else {
                logger.warn("There were unfinished bulk requests, consider increasing ttl.")
            }
        } catch (InterruptedException e) {
            logger.error("Exception while waiting for bulk operation to finish", e)
        } finally {
            bulkProcessor.close()
            if (!ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS)) {
                logger.warn("ThreadPool termination timed out!")
            }
            lowLevelClient.close()
            logger.info("Indexing was finished - ${indexingCounter.get()} documents were indexed!")
        }
    }

    private void initialize() {
        deleteIndex()
        createIndex()
    }

    private void deleteIndex() {
        try {
            lowLevelClient.performRequest("DELETE", "/" + INDEX)
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
            "number_of_shards": "5",
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
            lowLevelClient.performRequest("PUT", INDEX, Collections.emptyMap(), entity)
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
            lowLevelClient.performRequest("PUT", INDEX + "/_settings", Collections.emptyMap(), entity)
        } catch (Exception e) {
            logger.error("Error setting refresh_interval to default value", e)
        }
    }
}
