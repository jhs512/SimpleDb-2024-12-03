package org.example;

import lombok.Getter;

import java.time.LocalDateTime;

public class Article {
    @Getter
    private final long id;
    @Getter
    private final String title;
    @Getter
    private final String body;
    @Getter
    private final LocalDateTime createdDate;
    @Getter
    private final LocalDateTime modifiedDate;
    private final Boolean isBlind;

    public Article(long id, String title, String body, LocalDateTime createdDate, LocalDateTime modifiedDate, Boolean isBlind) {
        this.id = id;
        this.title = title;
        this.body = body;
        this.createdDate = createdDate;
        this.modifiedDate = modifiedDate;
        this.isBlind = isBlind;
    }

    public boolean isBlind() {
        return this.isBlind;
    }
}
