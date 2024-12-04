package com.ll;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Article {
    private Long id;

    private String title;
    private String body;

    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;

    //@JsonProperty("isBlind")
    private boolean blind;

}
