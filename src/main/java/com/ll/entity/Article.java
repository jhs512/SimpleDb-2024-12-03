package com.ll.entity;

import static lombok.AccessLevel.PRIVATE;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * packageName  : com.ll.entity
 * fileName     : Article
 * author       : Author
 * date         : 2024-12-03
 * description  :
 * ====================================================================================================
 * DATE           AUTHOR              NOTE
 * ----------------------------------------------------------------------------------------------------
 * 2024-12-03     Author     Initial creation.
 */
@Getter
@NoArgsConstructor(access = PRIVATE)
@AllArgsConstructor(access = PRIVATE)
public class Article {

    private Long          id;
    private String        title;
    private String        body;
    @JsonProperty("is_blind")
    private boolean       isBlind;
    @JsonProperty("created_date")
    private LocalDateTime createdDate;
    @JsonProperty("modified_date")
    private LocalDateTime modifiedDate;

    public static Article of(
            final Long id, final String title, final String body, final boolean isBlind,
            final LocalDateTime createdDate, final LocalDateTime modifiedDate
    ) {
        return new Article(id, title, body, isBlind, createdDate, modifiedDate);
    }

}
