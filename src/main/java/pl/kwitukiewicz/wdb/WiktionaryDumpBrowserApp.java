package pl.kwitukiewicz.wdb;

import de.tudarmstadt.ukp.jwktl.parser.WiktionaryDumpParser;
import pl.kwitukiewicz.wdb.elasticsearch.WiktionaryDumpEsRepository;
import pl.kwitukiewicz.wdb.parser.WiktionaryPageParser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

        WiktionaryDumpEsRepository wiktionaryDumpRepository = new WiktionaryDumpEsRepository();
        WiktionaryDumpParser dumpParser = new WiktionaryDumpParser(
                new WiktionaryPageParser(wiktionaryDumpRepository::indexObject));
        try {
        	wiktionaryDumpRepository.prepareIndexForRebuild();
	        dumpParser.parse(dumpFilePath.toFile());
	        wiktionaryDumpRepository.indexRebuildDone();
        } finally {
        	wiktionaryDumpRepository.cleanup();
        }
    }
}