package pl.kwitukiewicz.wdb.parser;

import de.tudarmstadt.ukp.jwktl.parser.IWiktionaryPageParser;
import de.tudarmstadt.ukp.jwktl.parser.util.IDumpInfo;
import pl.kwitukiewicz.wdb.model.WiktionaryPageDocument;

import java.util.Date;
import java.util.function.Consumer;

/**
 * @author Krzysztof Witukiewicz
 */
public class WiktionaryPageParser implements IWiktionaryPageParser {

    private Consumer<WiktionaryPageDocument> pageConsumer;
    private WiktionaryPageDocument currentPage;

    public WiktionaryPageParser(Consumer<WiktionaryPageDocument> pageConsumer) {
        this.pageConsumer = pageConsumer;
    }

    @Override
    public void onParserStart(IDumpInfo iDumpInfo) {

    }

    @Override
    public void onSiteInfoComplete(IDumpInfo iDumpInfo) {

    }

    @Override
    public void onParserEnd(IDumpInfo iDumpInfo) {

    }

    @Override
    public void onClose(IDumpInfo iDumpInfo) {

    }

    @Override
    public void onPageStart() {
        currentPage = new WiktionaryPageDocument();
    }

    @Override
    public void onPageEnd() {
        pageConsumer.accept(currentPage);
    }

    @Override
    public void setAuthor(String s) {

    }

    @Override
    public void setRevision(long l) {

    }

    @Override
    public void setTimestamp(Date date) {

    }

    @Override
    public void setPageId(long l) {

    }

    @Override
    public void setTitle(String title, String namespace) {
    	currentPage.setId(String.valueOf(namespace) + ":" + String.valueOf(title));
        currentPage.setTitle(title);
        currentPage.setNamespace(namespace);
    }

    @Override
    public void setText(String s) {
        currentPage.setText(s);
    }
}
