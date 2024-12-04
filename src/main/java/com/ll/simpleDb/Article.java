package com.ll.simpleDb;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Article {
    private Long id;
    private String title;
    private String body;
    private boolean isBlind;
    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;
}
