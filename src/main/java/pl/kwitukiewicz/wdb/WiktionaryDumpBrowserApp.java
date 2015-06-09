package pl.kwitukiewicz.wdb;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import de.tudarmstadt.ukp.jwktl.JWKTL;
import de.tudarmstadt.ukp.jwktl.api.IWiktionaryEdition;
import de.tudarmstadt.ukp.jwktl.api.IWiktionaryPage;
import de.tudarmstadt.ukp.jwktl.api.WiktionaryFormatter;
import de.tudarmstadt.ukp.jwktl.parser.WiktionaryDumpParser;
import pl.kwitukiewicz.wdb.elasticsearch.CreateIndexService;
import pl.kwitukiewicz.wdb.parser.WiktionaryPageParser;

/**
 * Created by Krzysztof Witukiewicz
 */
public class WiktionaryDumpBrowserApp {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Please provide path to an XML dump of wiktionary");
            return;
        }

        Path dumpFilePath = Paths.get(args[0]);
        if (!Files.exists(dumpFilePath)) {
            System.out.println("Provided file does not exist");
            return;
        }

        CreateIndexService createIndexService = new CreateIndexService();
        WiktionaryDumpParser dumpParser = new WiktionaryDumpParser(
                new WiktionaryPageParser(createIndexService::indexWiktionaryPage, createIndexService::cleanup));
        dumpParser.parse(dumpFilePath.toFile());
    }
}