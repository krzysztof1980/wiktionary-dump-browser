package pl.kwitukiewicz.wdb.elasticsearch

import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.elasticsearch.client.ResponseException

import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Krzysztof Witukiewicz
 */
abstract class ElasticsearchIndexingRepository extends ElasticsearchRepository {

    private static final long BULK_SIZE_BYTES = 15L * 1024 * 1024  // 15 MB

    private StringBuilder bulkBody
    private long bulkByteCount
    private int pendingDocs
    private final AtomicInteger indexingCounter = new AtomicInteger()

    ElasticsearchIndexingRepository(String index, String type) {
        super(index, type)
    }

    void prepareIndexForRebuild() {
        deleteIndex()
        createIndex()
        initializeBulk()
        indexingCounter.set(0)
    }

    synchronized void indexRebuildDone() {
        flushBulk()
        refreshIndex()
        logger.info("Indexing was finished - ${indexingCounter.get()} documents were indexed!")
    }

    @Override
    synchronized void cleanup() {
        if (bulkBody != null) {
            flushBulk()
            bulkBody = null
        }
        super.cleanup()
    }

    /**
     * May be called concurrently (the parser dispatches pages to a thread pool), so the
     * shared bulk state is guarded by the monitor. The old BulkProcessor handled this
     * internally; the new client has no auto-flushing bulk processor, so we accumulate
     * NDJSON bulk actions and flush them once the size threshold is reached.
     */
    synchronized void indexDocument(String id, String docJsonString) {
        bulkBody.append("{\"index\":{\"_index\":\"${index}\",\"_id\":\"${id}\"}}\n")
        bulkBody.append(docJsonString).append("\n")
        bulkByteCount += docJsonString.length()
        pendingDocs++
        if (bulkByteCount >= BULK_SIZE_BYTES) {
            flushBulk()
        }
    }

    private void initializeBulk() {
        bulkBody = new StringBuilder()
        bulkByteCount = 0
        pendingDocs = 0
    }

    private void flushBulk() {
        if (pendingDocs == 0) return
        try {
            Request request = new Request("POST", "/_bulk")
            request.setEntity(new NStringEntity(bulkBody.toString(), ContentType.APPLICATION_JSON))
            Response response = lowLevelClient.performRequest(request)
            String responseBody = EntityUtils.toString(response.getEntity())
            if (responseBody.contains('"errors":true')) {
                logger.error("Bulk request had partial failures: ${responseBody}")
            } else {
                int currCount = indexingCounter.addAndGet(pendingDocs)
                if (currCount % 10000 == 0) {
                    logger.info("Indexed ${currCount} documents so far!")
                }
            }
        } catch (Exception e) {
            logger.error("Error indexing documents", e)
        }
        bulkBody = new StringBuilder()
        bulkByteCount = 0
        pendingDocs = 0
    }

    private void deleteIndex() {
        try {
            lowLevelClient.performRequest(new Request("DELETE", "/${index}"))
            logger.info("Deleted index ${index}")
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
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
        Request request = new Request("PUT", index)
        request.setEntity(new NStringEntity(jsonString, ContentType.APPLICATION_JSON))
        try {
            lowLevelClient.performRequest(request)
            logger.info("Created index ${index}")
        } catch (Exception e) {
            throw new RuntimeException("Error creating index ${index}", e)
        }
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
        Request request = new Request("PUT", "${index}/_settings")
        request.setEntity(new NStringEntity(jsonString, ContentType.APPLICATION_JSON))
        try {
            lowLevelClient.performRequest(request)
        } catch (Exception e) {
            logger.error("Error setting refresh_interval to default value", e)
        }
    }
}
