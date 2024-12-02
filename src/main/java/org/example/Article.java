package org.example;

public class Article {
    private final long id;
    private String title;
    private String body;

    public Article(long id, String title, String body) {
        this.id = id;
        this.title = title;
        this.body = body;
    }
}
