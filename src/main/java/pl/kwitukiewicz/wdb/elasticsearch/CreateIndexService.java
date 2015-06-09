package pl.kwitukiewicz.wdb.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.Validate;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * @author Krzysztof Witukiewicz
 */
public class CreateIndexService {

    public static final String INDEX = "dewiktionary";
    public static final String TYPE = "page";

    private static final Logger logger = LoggerFactory.getLogger(CreateIndexService.class);

    private final Node node;
    private final Client client;
    private final BulkProcessor bulkProcessor;
    private final ObjectMapper objectMapper;
    private final BulkProcessor.Listener bulkProcessorListener = new BulkProcessor.Listener() {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            logger.debug("Starting bulk execution #{} with {} actions", executionId, request.numberOfActions());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                logger.warn(response.buildFailureMessage());
            } else {
                logger.debug("Finished bulk execution #{}", executionId);
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Exception was thrown during bulk execution " + executionId, failure);
        }
    };

    public CreateIndexService() {
        node = nodeBuilder().client(true).node();
        client = node.client();
        if (client.admin().indices().exists(new IndicesExistsRequest(INDEX)).actionGet().isExists()) {
            logger.info("Index '{}' exists, deleting...", INDEX);
            DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
            Validate.isTrue(delete.isAcknowledged(), "Index could not be deleted");
        }
        bulkProcessor = BulkProcessor.builder(
                client,
                bulkProcessorListener)
                .setBulkSize(new ByteSizeValue(15, ByteSizeUnit.MB))
                .build();
        objectMapper = new ObjectMapper();
    }

    public void indexWiktionaryPage(WiktionaryPageDocument document) {
        try {
            bulkProcessor.add(
                    new IndexRequest(INDEX, TYPE, document.getNamespace() + ":" + document.getTitle())
                            .opType(IndexRequest.OpType.CREATE)
                            .source(objectMapper.writeValueAsBytes(document)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanup() {
        logger.info("Cleaning up after indexing...");
        try {
            if (bulkProcessor.awaitClose(5, TimeUnit.MINUTES)) {
                logger.info("All bulk requests finished.");
            } else {
                logger.warn("There were unfinished bulk requests, consider increasing ttl.");
            }
            client.admin().indices().flush(new FlushRequest(INDEX)).actionGet();
        } catch (InterruptedException e) {
            logger.error("Exception while waiting for bulk operation to finish", e);
        } finally {
            node.close();
            client.close();
            logger.info("Indexing was finished!");
        }
    }
}
