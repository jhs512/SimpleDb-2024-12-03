package org.example.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;

@Getter
public class Article {

    private Long id;
    private String title;
    private String body;
    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;
    private boolean blind;

    private Article(Long id, String title, String body, LocalDateTime createdDate, LocalDateTime modifiedDate, boolean blind) {
        this.id = id;
        this.title = title;
        this.body = body;
        this.createdDate = createdDate;
        this.modifiedDate = modifiedDate;
        this.blind = blind;
    }

    public static Article of(Long id, String title, String body, LocalDateTime createdDate, LocalDateTime modifiedDate, boolean blind){
        return new Article(id, title, body, createdDate, modifiedDate, blind);

    }
}
