package com.ll.simpleDb;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class Article {
    long id;
    String title;
    String body;
    LocalDateTime createdDate;
    LocalDateTime modifiedDate;
    boolean isBlind;

    Article(long id, String title, String body, LocalDateTime createdDate, LocalDateTime modifiedDate, boolean isBlind) {
        this.id = id;
        this.title=title;
        this.body=body;
        this.createdDate=createdDate;
        this.modifiedDate=modifiedDate;
        this.isBlind=isBlind;
    }

    static Article convert(Map<String, Object> map) {
        long id = (long) map.get("id");
        String title = (String) map.get("title");
        String body = (String) map.get("body");
        LocalDateTime createdDate = (LocalDateTime) map.get("createdDate");
        LocalDateTime modifiedDate = (LocalDateTime) map.get("modifiedDate");
        boolean isBlind = (boolean) map.get("isBlind");
        return new Article(id,title,body,createdDate,modifiedDate,isBlind);
    }

    long getId() {
        return id;
    }

    String getTitle() {
        return title;
    }

    String getBody() {
        return body;
    }

    LocalDateTime getCreatedDate() {
        return createdDate;
    }

    LocalDateTime getModifiedDate() {
        return modifiedDate;
    }

    boolean isBlind() {
        return isBlind;
    }
}
