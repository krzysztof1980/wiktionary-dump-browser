package pl.kwitukiewicz.wdb.model
/**
 * @author Krzysztof Witukiewicz
 */
class WiktionaryPageDocument {

    String namespace
    String title
    String text

    @Override
    String toString() {
        return "WiktionaryPageDocument{" +
                "namespace='" + namespace + '\'' +
                ", title='" + title + '\'' +
                '}'
    }
}
