package pl.kwitukiewicz.wdb.model

import pl.kwitukiewicz.wdb.elasticsearch.Indexable

/**
 * @author Krzysztof Witukiewicz
 */
class WiktionaryPageDocument implements Indexable {

	String id
    String namespace
    String title
    String text

    @Override
    String toString() {
        return "WiktionaryPageDocument{" +
                "id='" + id + '\'' +
                ", namespace='" + namespace + '\'' +
                ", title='" + title + '\'' +
                '}'
    }
}
