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

    // TODO: 어노테이션 제거하면 jackson 쓸때  불일치 문제..
    @JsonProperty("isBlind")
    private boolean isBlind;

}
