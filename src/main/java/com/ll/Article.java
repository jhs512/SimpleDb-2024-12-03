package com.ll;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class Article {

    /**
     * id INT UNSIGNED NOT NULL AUTO_INCREMENT,
     *                     PRIMARY KEY(id),
     *                     createdDate DATETIME NOT NULL,
     *                     modifiedDate DATETIME NOT NULL,
     *                     title VARCHAR(100) NOT NULL,
     *                     `body` TEXT NOT NULL,
     *                     isBlind BIT(1) NOT NULL DEFAULT 0
     */

    private long id;
    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;
    private String title;
    private String body;
    private boolean isBlind;

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Article{");
        sb.append("id=").append(id);
        sb.append(", createdDate=").append(createdDate);
        sb.append(", modifiedDate=").append(modifiedDate);
        sb.append(", title='").append(title).append('\'');
        sb.append(", body='").append(body).append('\'');
        sb.append(", isBlind=").append(isBlind);
        sb.append('}');
        return sb.toString();
    }
}