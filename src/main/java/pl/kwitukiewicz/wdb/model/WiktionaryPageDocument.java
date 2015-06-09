package pl.kwitukiewicz.wdb.model;

import java.util.List;

/**
 * @author Krzysztof Witukiewicz
 */
public class WiktionaryPageDocument {

    private String namespace;
    private String title;
    private String[] text;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String[] getText() {
        return text;
    }

    public void setText(String[] text) {
        this.text = text;
    }
}
