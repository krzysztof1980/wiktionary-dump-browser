package pl.kwitukiewicz.wdb.elasticsearch

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpEntity
import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.ByteSizeUnit
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.threadpool.ThreadPool

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Krzysztof Witukiewicz
 */
abstract class ElasticsearchIndexingRepository<T extends Indexable> extends ElasticsearchRepository {

    private ThreadPool threadPool
    private BulkProcessor bulkProcessor
    private BulkProcessor.Listener bulkProcessorListener

    private final AtomicInteger indexingCounter = new AtomicInteger()

    ElasticsearchIndexingRepository(String index, String type) {
        super(index, type)
        bulkProcessorListener = new BulkProcessorListener(indexingCounter)
    }

    void prepareIndexForRebuild() {
        deleteIndex()
        createIndex()
        initializeBulkProcessor()
        indexingCounter.set(0)
    }

    void indexRebuildDone() {
        refreshIndex()
        closeBulkProcessor()
        logger.info("Indexing was finished - ${indexingCounter.get()} documents were indexed!")
    }

    @Override
    void cleanup() {
        if (bulkProcessor != null)
            closeBulkProcessor()
        super.cleanup()
    }

    void indexObject(T obj) {
        ObjectMapper objectMapper = new ObjectMapper()
        try {
            bulkProcessor.add(new IndexRequest(index, type, obj.id)
                                      .opType(DocWriteRequest.OpType.CREATE)
                                      .source(objectMapper.writeValueAsBytes(obj), XContentType.JSON))
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e)
        }
    }

    private void deleteIndex() {
        try {
            lowLevelClient.performRequest("DELETE", "/${index}")
            logger.info("Deleted index ${index}")
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                logger.info("Index ${index} does not exist - nothing to delete.")
            } else {
                throw new RuntimeException("Unexpected error deleting index ${index}", e)
            }
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error deleting index ${index}", e)
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
    }
}"""
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON)
        try {
            lowLevelClient.performRequest("PUT", index, Collections.emptyMap(), entity)
            logger.info("Created index ${index}")
        } catch (Exception e) {
            throw new RuntimeException("Error creating index ${index}", e)
        }
    }

    private void initializeBulkProcessor() {
        threadPool = new ThreadPool(Settings.builder().put("node.name", "dummy").build())
        bulkProcessor = new BulkProcessor.Builder({ r, l -> client.bulkAsync(r, l) },
                                                  bulkProcessorListener,
                                                  threadPool)
                .setBulkSize(new ByteSizeValue(15, ByteSizeUnit.MB))
                .build()
    }

    private void closeBulkProcessor() {
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
        }
        bulkProcessor = null
        threadPool = null
    }

    private void refreshIndex() {
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
            lowLevelClient.performRequest("PUT", "${index}/_settings", Collections.emptyMap(), entity)
        } catch (Exception e) {
            logger.error("Error setting refresh_interval to default value", e)
        }
    }

    private class BulkProcessorListener implements BulkProcessor.Listener {
        private final AtomicInteger indexingCounter

        BulkProcessorListener(AtomicInteger indexingCounter) {
            this.indexingCounter = indexingCounter
        }

        @Override
        void beforeBulk(long executionId, BulkRequest request) {
            // do nothing
        }

        @Override
        void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage())
            } else {
                int currCount = indexingCounter.addAndGet(request.numberOfActions())
                if (currCount % 10000 == 0) {
                    logger.info("Indexed ${currCount} documents so far!")
                }
            }
        }

        @Override
        void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Exception was thrown during bulk execution " + executionId, failure)
        }
    }
}
