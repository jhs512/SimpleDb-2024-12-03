package com.ll;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class Article {
    long id;
    String title;
    String body;
    LocalDateTime createdDate;
    LocalDateTime modifiedDate;
    boolean isBlind;

    public void setIsBlind(Object isBlind) {
        if (isBlind instanceof Boolean) {
            this.isBlind = (Boolean) isBlind;
        } else if (isBlind instanceof Number) {
            this.isBlind = ((Number) isBlind).intValue() != 0;
        }
    }
}
