package com.ll.simpleDb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
public class Article {

    long id;
    LocalDateTime createdDate;
    LocalDateTime modifiedDate;
    String title;
    String body;
    boolean isBlind;

    @JsonCreator
    public Article(@JsonProperty("id")long id, @JsonProperty("createdDate")LocalDateTime createdDate,
                   @JsonProperty("modifiedDate")LocalDateTime modifiedDate, @JsonProperty("title")String title,
                   @JsonProperty("body")String body, @JsonProperty("isBlind")boolean isBlind){
        this.id = id;
        this.createdDate = createdDate;
        this.modifiedDate = modifiedDate;
        this.title = title;
        this.body = body;
        this.isBlind = isBlind;
    }

}
