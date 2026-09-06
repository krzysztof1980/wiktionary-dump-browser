package pl.kwitukiewicz.wdb.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument

import java.net.URLEncoder

/**
 * @author Krzysztof Witukiewicz
 */
class WiktionaryDumpEsRepository extends ElasticsearchIndexingRepository {
    private static final Logger logger = LoggerFactory.getLogger(WiktionaryDumpEsRepository.class)

    private static final String WIKTIONARY_DUMP_INDEX = "dewiktionary_dump"
    // Retained for API compatibility; ES 8 removed mapping types
    private static final String WIKTIONARY_DUMP_TYPE = "page"

    private final ObjectMapper objectMapper = new ObjectMapper()

    WiktionaryDumpEsRepository() {
        super(WIKTIONARY_DUMP_INDEX, WIKTIONARY_DUMP_TYPE)
    }

    List<WiktionaryPageDocument> findPageByTitle(String title) {
        def response = client.search({ s ->
            s.index(index)
             .query({ q -> q.term({ t -> t.field("title.keyword").value(title) }) })
        }, WiktionaryPageDocument)
        return response.hits().hits().collect { it.source() }
    }

    WiktionaryPageDocument findPageById(String id) {
        id = URLEncoder.encode(id, "UTF-8")
        def response = client.get({ g -> g.index(index).id(id) }, WiktionaryPageDocument)
        return response.found() ? response.source() : null
    }

    void indexWiktionaryPage(WiktionaryPageDocument doc) {
        def id = String.valueOf(doc.namespace) + ":" + String.valueOf(doc.title)
        def docJson = objectMapper.writeValueAsString(doc)
        indexDocument(id, docJson)
    }

    @Override
    protected Logger getLogger() {
        return logger
    }
}
