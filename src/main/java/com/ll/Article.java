package com.ll;

import lombok.Getter;

import java.time.LocalDateTime;


public class Article {

    @Getter
    long id;

    @Getter
    LocalDateTime createdDate;

    @Getter
    LocalDateTime modifiedDate;

    @Getter
    String title;

    @Getter
    String body;

    @Getter
    boolean isBlind;

    public Article(long id, LocalDateTime createdDate, LocalDateTime modifiedDate, String title, String body, boolean isBlind){
        this.id = id;
        this.createdDate = createdDate;
        this.modifiedDate = modifiedDate;
        this.title = title;
        this.body = body;
        this.isBlind = isBlind;
    }

}
