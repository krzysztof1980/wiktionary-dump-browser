package pl.kwitukiewicz.wdb.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument

import java.util.stream.Collectors

/**
 * @author Krzysztof Witukiewicz
 */
class WiktionaryDumpEsRepository extends ElasticsearchIndexingRepository {
    private static final Logger logger = LoggerFactory.getLogger(WiktionaryDumpEsRepository.class)

    private static final String WIKTIONARY_DUMP_INDEX = "dewiktionary_dump"
    private static final String WIKTIONARY_DUMP_TYPE = "page"

    private final ObjectMapper objectMapper = new ObjectMapper()

    WiktionaryDumpEsRepository() {
        super(WIKTIONARY_DUMP_INDEX, WIKTIONARY_DUMP_TYPE)
    }

    List<WiktionaryPageDocument> findPageByTitle(String title) {
        def searchRequest = new SearchRequest(index)
        searchRequest.types(type)

        def sourceBuilder = new SearchSourceBuilder()
        sourceBuilder.query(QueryBuilders.termQuery("title.keyword", title))
        searchRequest.source(sourceBuilder)

        def response = client.search(searchRequest)
        def objectMapper = new ObjectMapper()
        return Arrays.stream(response.getHits().hits)
                     .map({ h -> h.getSourceAsString() })
                     .map({ docString -> objectMapper.readValue(docString, WiktionaryPageDocument) })
                     .collect(Collectors.toList())
    }

    WiktionaryPageDocument findPageById(String id) {
        id = URLEncoder.encode(id, "UTF-8")
        def getRequest = new GetRequest(index, type, id)
        def response = client.get(getRequest)
        return response.isExists() ? objectMapper.readValue(response.sourceAsString, WiktionaryPageDocument) : null
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
