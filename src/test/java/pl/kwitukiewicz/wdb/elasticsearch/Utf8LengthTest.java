package pl.kwitukiewicz.wdb.elasticsearch;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Utf8LengthTest {

    @Test
    void matchesJavaEncoder() {
        String[] samples = {
                "ascii only",
                "Müller",
                "Österreichische KuKuks",
                "€¥₽",
                "Müller €\uD83D\uDE00",           // umlaut + euro + surrogate-pair emoji
                "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02",
                "",
                "\n{\"index\":{\"_index\":\"i\",\"_id\":\"Wort\"}}\n"
        };
        for (String s : samples)
            assertEquals(s.getBytes(StandardCharsets.UTF_8).length, Utf8LengthProbe.of(s),
                         "for: " + s);
    }

    @Test
    void knownValues() {
        assertEquals(7, Utf8LengthProbe.of("Müller"));  // 5 ASCII + 2-byte ü
        assertEquals(3, Utf8LengthProbe.of("€"));
        assertEquals(4, Utf8LengthProbe.of("\uD83D\uDE00"));
    }

    // utf8Length is package-private in ElasticsearchIndexingRepository; call it through a
    // tiny wrapper so the test pins the production method itself.
    private static final class Utf8LengthProbe {
        static int of(String s) {
            return ElasticsearchIndexingRepository.utf8Length(s);
        }
    }
}
