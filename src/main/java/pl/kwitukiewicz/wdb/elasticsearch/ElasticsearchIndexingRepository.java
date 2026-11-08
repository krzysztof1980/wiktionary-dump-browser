package pl.kwitukiewicz.wdb.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Krzysztof Witukiewicz
 */
public abstract class ElasticsearchIndexingRepository extends ElasticsearchRepository {

    private static final long BULK_SIZE_BYTES = 15L * 1024 * 1024;  // 15 MB
    private static final int PER_BULK_ERROR_LOG_LIMIT = 5;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private StringBuilder bulkBody;
    private long bulkByteCount;
    private int pendingDocs;
    private final AtomicInteger indexingCounter = new AtomicInteger();
    private final AtomicInteger failedDocsCounter = new AtomicInteger();

    private record Bulk(String ndjson, int docs) { }

    private static final Object FLUSH_SENTINEL = new Object();
    // One bulk in flight: when the flusher lags, indexDocument blocks - deliberate
    // backpressure instead of unbounded buffering.
    private final BlockingQueue<Object> bulkQueue = new ArrayBlockingQueue<>(1);
    private Thread bulkFlusher;

    public ElasticsearchIndexingRepository(String index, String type) {
        super(index, type);
    }

    public void prepareIndexForRebuild() {
        deleteIndex();
        createIndex();
        initializeBulk();
        bulkFlusher = new Thread(this::flusherLoop, "es-bulk-flusher");
        bulkFlusher.setDaemon(true);
        bulkFlusher.start();
        indexingCounter.set(0);
        failedDocsCounter.set(0);
    }

    public synchronized void indexRebuildDone() {
        stopBulkBuffering();
        refreshIndex();
        int indexed = indexingCounter.get();
        int failed = failedDocsCounter.get();
        getLogger().info("Indexing was finished - " + indexed + " documents were indexed"
                         + (failed > 0 ? ", " + failed + " FAILED" : "") + "!");
    }

    @Override
    public synchronized void cleanup() throws IOException {
        stopBulkBuffering();
        super.cleanup();
    }

    /**
     * May be called concurrently (the parser dispatches pages to a thread pool), so the
     * shared buffer state is guarded by the monitor. The monitor is held only for appending
     * and swapping the buffer: the finished bulk is handed to a single flusher thread, so
     * the multi-megabyte _bulk POST never blocks parser threads - except when the flusher
     * lags, which is deliberate backpressure. The old BulkProcessor handled this internally;
     * the new client has no auto-flushing bulk processor.
     */
    public synchronized void indexDocument(String id, String docJsonString) {
        if (bulkBody == null)
            throw new IllegalStateException(
                    "indexDocument called outside of prepareIndexForRebuild..indexRebuildDone on index " + index);
        String action = "{\"index\":{\"_index\":\"" + index + "\",\"_id\":\"" + id + "\"}}\n";
        bulkBody.append(action);
        bulkBody.append(docJsonString).append("\n");
        bulkByteCount += utf8Length(action) + utf8Length(docJsonString) + 1;
        pendingDocs++;
        if (bulkByteCount >= BULK_SIZE_BYTES) {
            Bulk ready = new Bulk(bulkBody.toString(), pendingDocs);
            initializeBulk();
            try {
                bulkQueue.put(ready);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while handing bulk to the flusher", e);
            }
        }
    }

    private void initializeBulk() {
        bulkBody = new StringBuilder();
        bulkByteCount = 0;
        pendingDocs = 0;
    }

    private void sendBulk(String ndjson, int docs) {
        int ok = 0;
        int failed = 0;
        try {
            Request request = new Request("POST", "/_bulk");
            request.setEntity(new NStringEntity(ndjson, ContentType.APPLICATION_JSON));
            Response response = lowLevelClient.performRequest(request);
            JsonNode root = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
            if (root.path("errors").asBoolean(false)) {
                for (JsonNode item : root.path("items")) {
                    JsonNode result = item.path("index");
                    JsonNode error = result.path("error");
                    if (error.isMissingNode()) {
                        ok++;
                    } else {
                        failed++;
                        if (failed <= PER_BULK_ERROR_LOG_LIMIT) {
                            getLogger().error("Bulk item failed [id={}]: {} - {}",
                                              result.path("_id").asText(),
                                              error.path("type").asText(), error.path("reason").asText());
                        }
                    }
                }
                if (failed > PER_BULK_ERROR_LOG_LIMIT)
                    getLogger().error("...and {} more failed items in this bulk", failed - PER_BULK_ERROR_LOG_LIMIT);
                getLogger().error("Bulk finished with failures: {} indexed, {} failed", ok, failed);
            } else {
                ok = docs;
            }
        } catch (Exception e) {
            failed = docs;
            getLogger().error("Bulk request failed - all {} documents of this bulk are lost", docs, e);
        }
        if (ok > 0) {
            int before = indexingCounter.getAndAdd(ok);
            if (before / 10000 != (before + ok) / 10000)
                getLogger().info("Indexed " + (before + ok) + " documents so far!");
        }
        if (failed > 0)
            failedDocsCounter.addAndGet(failed);
    }

    /**
     * UTF-8 encoded byte length of s - what ES actually counts for the 15 MB bulk threshold.
     * Cheaper than getBytes(UTF_8): no array allocation per document.
     */
    static int utf8Length(String s) {
        int bytes = s.length();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 0x80) {
                if (Character.isHighSurrogate(c)) {
                    bytes += 2;   // pair encodes as 4 bytes for 2 chars
                    i++;          // skip the low surrogate
                } else if (!Character.isLowSurrogate(c)) {
                    bytes += (c < 0x800) ? 1 : 2;
                }
            }
        }
        return bytes;
    }

    private void flusherLoop() {
        try {
            while (true) {
                Object item = bulkQueue.take();
                if (item == FLUSH_SENTINEL)
                    return;
                Bulk bulk = (Bulk) item;
                sendBulk(bulk.ndjson(), bulk.docs());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().error("Bulk flusher interrupted; " + bulkQueue.size() + " item(s) never sent", e);
        }
    }

    /**
     * Sends whatever is still buffered, stops the flusher and waits for it.
     * Safe to call twice (indexRebuildDone then cleanup).
     */
    private void stopBulkBuffering() {
        if (bulkFlusher == null)
            return;
        try {
            if (bulkBody != null && pendingDocs > 0)
                bulkQueue.put(new Bulk(bulkBody.toString(), pendingDocs));
            bulkQueue.put(FLUSH_SENTINEL);
            bulkFlusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().error("Interrupted while waiting for the bulk flusher to finish", e);
        }
        bulkBody = null;
        bulkByteCount = 0;
        pendingDocs = 0;
        bulkFlusher = null;
    }

    private void deleteIndex() {
        try {
            lowLevelClient.performRequest(new Request("DELETE", "/" + index));
            getLogger().info("Deleted index " + index);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                getLogger().info("Index " + index + " does not exist - nothing to delete.");
            } else {
                throw new RuntimeException("Unexpected error deleting index " + index, e);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error deleting index " + index, e);
        }
    }

    private void createIndex() {
        String jsonString = """
{
    "settings": {
        "index": {
            "number_of_shards": "5",
            "number_of_replicas": "0",
            "refresh_interval": "-1"
        }
    }
}""";
        Request request = new Request("PUT", index);
        request.setEntity(new NStringEntity(jsonString, ContentType.APPLICATION_JSON));
        try {
            lowLevelClient.performRequest(request);
            getLogger().info("Created index " + index);
        } catch (Exception e) {
            throw new RuntimeException("Error creating index " + index, e);
        }
    }

    private void refreshIndex() {
        String jsonString = """
{
    "settings": {
        "index": {
            "refresh_interval": "1s"
        }
    }
}""";
        Request request = new Request("PUT", index + "/_settings");
        request.setEntity(new NStringEntity(jsonString, ContentType.APPLICATION_JSON));
        try {
            lowLevelClient.performRequest(request);
        } catch (Exception e) {
            getLogger().error("Error setting refresh_interval to default value", e);
        }
    }
}
