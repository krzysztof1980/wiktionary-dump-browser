package pl.kwitukiewicz.wdb.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Krzysztof Witukiewicz
 */
public class WiktionaryDumpEsRepository extends ElasticsearchIndexingRepository {
    private static final Logger logger = LoggerFactory.getLogger(WiktionaryDumpEsRepository.class);

    private static final String WIKTIONARY_DUMP_INDEX = "dewiktionary_dump";
    // Retained for API compatibility; ES 8 removed mapping types
    private static final String WIKTIONARY_DUMP_TYPE = "page";

    private final ObjectMapper objectMapper = new ObjectMapper();

    public WiktionaryDumpEsRepository() {
        super(WIKTIONARY_DUMP_INDEX, WIKTIONARY_DUMP_TYPE);
    }

    public List<WiktionaryPageDocument> findPageByTitle(String title) throws IOException {
        return client.search(s -> s
                        .index(index)
                        .query(q -> q.term(t -> t.field("title.keyword").value(title))),
                WiktionaryPageDocument.class)
                .hits().hits().stream()
                .map(hit -> hit.source())
                .collect(Collectors.toList());
    }

    public WiktionaryPageDocument findPageById(String id) throws IOException {
        String encodedId = URLEncoder.encode(id, "UTF-8");
        var response = client.get(g -> g.index(index).id(encodedId), WiktionaryPageDocument.class);
        return response.found() ? response.source() : null;
    }

    public void indexWiktionaryPage(WiktionaryPageDocument doc) {
        String id = String.valueOf(doc.getNamespace()) + ":" + String.valueOf(doc.getTitle());
        String docJson;
        try {
            docJson = objectMapper.writeValueAsString(doc);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing document " + id, e);
        }
        indexDocument(id, docJson);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
