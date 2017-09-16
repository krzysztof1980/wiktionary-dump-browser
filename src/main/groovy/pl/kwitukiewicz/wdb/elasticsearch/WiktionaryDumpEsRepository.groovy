package pl.kwitukiewicz.wdb.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper
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
class WiktionaryDumpEsRepository extends ElasticsearchIndexingRepository<WiktionaryPageDocument> {
    private static final Logger logger = LoggerFactory.getLogger(WiktionaryDumpEsRepository.class)

    private static final String WIKTIONARY_DUMP_INDEX = "dewiktionary_dump"
    private static final String WIKTIONARY_DUMP_TYPE = "page"

    WiktionaryDumpEsRepository() {
        super(WIKTIONARY_DUMP_INDEX, WIKTIONARY_DUMP_TYPE)
    }

    List<WiktionaryPageDocument> findEntryByTitle(String title) {
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

    @Override
    protected Logger getLogger() {
        return logger
    }
}
