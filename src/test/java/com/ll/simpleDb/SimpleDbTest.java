package com.ll.simpleDb;

import static org.assertj.core.api.Assertions.assertThat;

import com.ll.entity.Article;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * packageName  : com.ll.simpleDb
 * fileName     : SimpleDbTest
 * author       : Author
 * date         : 2024-12-03
 * description  :
 * ====================================================================================================
 * DATE           AUTHOR              NOTE
 * ----------------------------------------------------------------------------------------------------
 * 2024-12-03     Author              Initial creation.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
class SimpleDbTest {

    private static SimpleDb simpleDb;

    @BeforeAll
    static void beforeAll() {
        simpleDb = new SimpleDb("localhost", 3306, "simpleDb", "lldj123414", "simpleDb__test");
//        simpleDb.setDevMode(true);

        createArticleTable();
    }

    @BeforeEach
    void beforeEach() {
        truncateArticleTable();
        makeArticleTestData();
    }

    private static void createArticleTable() {
        simpleDb.run("DROP TABLE IF EXISTS article");
        simpleDb.run("""
                     CREATE TABLE article(
                        id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        title VARCHAR(100) NOT NULL,
                        `body` TEXT NOT NULL,
                        is_blind BIT(1) NOT NULL DEFAULT 0,
                        created_date DATETIME NOT NULL,
                        modified_date DATETIME NOT NULL
                     );
                     """);
    }

    private void truncateArticleTable() {
        simpleDb.run("TRUNCATE article");
    }

    private void makeArticleTestData() {
        IntStream.rangeClosed(1, 6).forEach(n -> {
            boolean isBlind = n > 3;
            String  title   = "제목%d".formatted(n);
            String  body    = "내용%d".formatted(n);

            simpleDb.run("""
                         INSERT INTO article
                         SET title = ?,
                             `body` = ?,
                             is_blind = ?,
                             created_date = NOW(),
                             modified_date = NOW()
                         """, title, body, isBlind);
        });
    }

    @Test
    @DisplayName("insert")
    void t001() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        INSERT INTO article
        SET created_date = NOW(),
            modified_date = NOW(),
            title = '제목 new',
            body = '내용 new'
         */
        sql.append("INSERT INTO article")
           .append("SET created_date = NOW()")
           .append(", modified_date = NOW()")
           .append(", title = ?", "제목 new")
           .append(", body = ?", "내용 new");

        //When
        long newId = sql.insert();

        //Then
        assertThat(newId).isGreaterThan(0);
    }

    @Test
    @DisplayName("update")
    void t002() {
        //Given
        Sql sql = simpleDb.genSql();
        // id가 0, 1, 2, 3인 글 수정
        // id가 0인 글은 없으니, 실제로는 3개의 글이 수정됨
        /*
        == rawSql ==
        UPDATE article
        SET title = '제목 new'
        WHERE id IN ('0', '1', '2', '3')
         */
        sql.append("UPDATE article")
           .append("SET title = ?", "제목 new")
           .append("WHERE id IN (?, ?, ?, ?)", 0, 1, 2, 3);

        //When
        int affectedRowsCount = sql.update();

        //Then
        assertThat(affectedRowsCount).isEqualTo(3);
    }

    @Test
    @DisplayName("delete")
    void t003() {
        //Given
        Sql sql = simpleDb.genSql();
        // id가 0, 1, 3인 글 삭제
        // id가 0인 글은 없으니, 실제로는 2개의 글이 삭제됨
        /*
        == rawSql ==
        DELETE FROM article
        WHERE id IN ('0', '1', '3')
         */
        sql.append("DELETE")
           .append("FROM article")
           .append("WHERE id IN (?, ?, ?)", 0, 1, 3);

        //When
        int affectedRowsCount = sql.delete();

        //Then
        assertThat(affectedRowsCount).isEqualTo(2);
    }

    @Test
    @DisplayName("selectRows")
    void t004() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        ORDER BY id ASC
        LIMIT 3
         */
        sql.append("SELECT * FROM article ORDER BY id ASC LIMIT 3");

        //When
        List<Map<String, Object>> articleRows = sql.selectRows();

        //Then
        IntStream.range(0, articleRows.size()).forEach(i -> {
            long id = i + 1;

            Map<String, Object> articleRow = articleRows.get(i);

            assertThat(articleRow.get("id")).isEqualTo(id);
            assertThat(articleRow.get("title")).isEqualTo("제목%d".formatted(id));
            assertThat(articleRow.get("body")).isEqualTo("내용%d".formatted(id));
            assertThat(articleRow.get("is_blind")).isEqualTo(false);
            assertThat(articleRow.get("created_date")).isInstanceOf(LocalDateTime.class);
            assertThat(articleRow.get("created_date")).isNotNull();
            assertThat(articleRow.get("modified_date")).isInstanceOf(LocalDateTime.class);
            assertThat(articleRow.get("modified_date")).isNotNull();
        });
    }

    @Test
    @DisplayName("selectRow")
    void t005() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        WHERE id = 1
         */
        sql.append("SELECT * FROM article WHERE id = 1");

        //When
        Map<String, Object> articleRow = sql.selectRow();

        //Then
        assertThat(articleRow.get("id")).isEqualTo(1L);
        assertThat(articleRow.get("title")).isEqualTo("제목1");
        assertThat(articleRow.get("body")).isEqualTo("내용1");
        assertThat(articleRow.get("is_blind")).isEqualTo(false);
        assertThat(articleRow.get("created_date")).isInstanceOf(LocalDateTime.class);
        assertThat(articleRow.get("created_date")).isNotNull();
        assertThat(articleRow.get("modified_date")).isInstanceOf(LocalDateTime.class);
        assertThat(articleRow.get("modified_date")).isNotNull();
    }

    @Test
    @DisplayName("selectDatetime")
    void t006() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT NOW()
         */
        sql.append("SELECT NOW()");

        //When
        LocalDateTime datetime = sql.selectDatetime();

        //Then
        long diff = ChronoUnit.SECONDS.between(datetime, LocalDateTime.now());

        assertThat(diff).isLessThanOrEqualTo(1L);
    }

    @Test
    @DisplayName("selectLong")
    void t007() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT id
        FROM article
        WHERE id = 1
         */
        sql.append("SELECT id")
           .append("FROM article")
           .append("WHERE id = 1");

        //When
        Long id = sql.selectLong();

        //Then
        assertThat(id).isEqualTo(1);
    }

    @Test
    @DisplayName("selectString")
    void t008() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT title
        FROM article
        WHERE id = 1
         */
        sql.append("SELECT title")
           .append("FROM article")
           .append("WHERE id = 1");

        //When
        String title = sql.selectString();

        //Then
        assertThat(title).isEqualTo("제목1");
    }

    @Test
    @DisplayName("selectBoolean")
    void t009() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT is_blind
        FROM article
        WHERE id = 1
         */
        sql.append("SELECT is_blind")
           .append("FROM article")
           .append("WHERE id = 1");

        //When
        Boolean isBlind = sql.selectBoolean();

        //Then
        assertThat(isBlind).isFalse();
    }

    @Test
    @DisplayName("selectBoolean, 2nd")
    void t010() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT 1 = 1
         */
        sql.append("SELECT 1 = 1");

        //When
        Boolean flag = sql.selectBoolean();

        //Then
        assertThat(flag).isTrue();
    }

    @Test
    @DisplayName("selectBoolean, 3rd")
    void t011() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT 1 = 0
         */
        sql.append("SELECT 1 = 0");

        //When
        Boolean flag = sql.selectBoolean();

        //Then
        assertThat(flag).isFalse();
    }

    @Test
    @DisplayName("select, LIKE 사용법")
    void t012() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT COUNT(*)
        FROM article
        WHERE id BETWEEN '1' AND '3'
        AND title LIKE CONCAT('%', '제목', '%')
         */
        sql.append("SELECT COUNT(*)")
           .append("FROM article")
           .append("WHERE id BETWEEN ? AND ?", 1, 3)
           .append("AND title LIKE CONCAT('%', ?, '%')", "제목");

        //When
        long count = sql.selectLong();

        //Then
        assertThat(count).isEqualTo(3);
    }

    @Test
    @DisplayName("appendIn")
    void t013() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT COUNT(*)
        FROM article
        WHERE id IN ('1', '2', '3')
         */
        sql.append("SELECT COUNT(*)")
           .append("FROM article")
           .appendIn("WHERE id IN (?)", 1, 2, 3);

        //When
        long count = sql.selectLong();

        //Then
        assertThat(count).isEqualTo(3);
    }

    @Test
    @DisplayName("selectLongs, ORDER BY FIELD 사용법")
    void t014() {
        //Given
        Long[] ids = {2L, 1L, 3L};

        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT id
        FROM article
        WHERE id IN ('2', '3', '1')
        ORDER BY FIELD (id, '2', '3', '1')
         */
        sql.append("SELECT id")
           .append("FROM article")
           .appendIn("WHERE id IN (?)", ids)
           .appendIn("ORDER BY FIELD (id, ?)", ids);

        //When
        List<Long> foundIds = sql.selectLongs();

        //Then
        assertThat(foundIds).isEqualTo(Arrays.stream(ids).toList());
    }

    @Test
    @DisplayName("selectRows, Article")
    void t015() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        ORDER BY id ASC
        LIMIT 3
         */
        sql.append("SELECT * FROM article ORDER BY id ASC LIMIT 3");

        //When
        List<Article> articleRows = sql.selectRows(Article.class);

        //Then
        IntStream.range(0, articleRows.size()).forEach(i -> {
            long id = i + 1;

            Article article = articleRows.get(i);

            assertThat(article.getId()).isEqualTo(id);
            assertThat(article.getTitle()).isEqualTo("제목%d".formatted(id));
            assertThat(article.getBody()).isEqualTo("내용%d".formatted(id));
            assertThat(article.getCreatedDate()).isInstanceOf(LocalDateTime.class);
            assertThat(article.getCreatedDate()).isNotNull();
            assertThat(article.getModifiedDate()).isInstanceOf(LocalDateTime.class);
            assertThat(article.getModifiedDate()).isNotNull();
            assertThat(article.isBlind()).isFalse();
        });
    }

    @Test
    @DisplayName("selectRow, Article")
    void t016() {
        //Given
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        WHERE id = 1
         */
        sql.append("SELECT * FROM article WHERE id = 1");

        //When
        Article articleRow = sql.selectRow(Article.class);

        //Then
        long id = 1;

        assertThat(articleRow.getId()).isEqualTo(id);
        assertThat(articleRow.getTitle()).isEqualTo("제목1");
        assertThat(articleRow.getBody()).isEqualTo("내용1");
        assertThat(articleRow.getCreatedDate()).isInstanceOf(LocalDateTime.class);
        assertThat(articleRow.getCreatedDate()).isNotNull();
        assertThat(articleRow.getModifiedDate()).isInstanceOf(LocalDateTime.class);
        assertThat(articleRow.getModifiedDate()).isNotNull();
        assertThat(articleRow.isBlind()).isFalse();
    }

    @Test
    @DisplayName("use in multi threading")
    void t017() throws InterruptedException {
        //Given
        // 쓰레드 풀의 크기를 정의합니다.
        int numberOfThreads = 10;

        // 고정 크기의 쓰레드 풀을 생성합니다.
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        // 성공한 작업의 수를 세는 원자적 카운터를 생성합니다.
        AtomicInteger successCounter = new AtomicInteger(0);

        // 동시에 실행되는 작업의 수를 세는 데 사용되는 래치를 생성합니다.
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        // 각 쓰레드에서 실행될 작업을 정의합니다.
        Runnable task = () -> {
            try {
                // SimpleDb에서 SQL 객체를 생성합니다.
                Sql sql = simpleDb.genSql();

                // SQL 쿼리를 작성합니다.
                sql.append("SELECT * FROM article WHERE id = 1");

                // 쿼리를 실행하여 결과를 Article 객체로 매핑합니다.
                Article article = sql.selectRow(Article.class);

                // 기대하는 Article 객체의 ID를 정의합니다.
                Long id = 1L;

                // Article 객체의 값이 기대하는 값과 일치하는지 확인하고,
                // 일치하는 경우 성공 카운터를 증가시킵니다.
                if (article.getId() == id
                    && article.getTitle().equals("제목%d".formatted(id))
                    && article.getBody().equals("내용%d".formatted(id))
                    && article.getCreatedDate() != null
                    && article.getModifiedDate() != null
                    && !article.isBlind())
                    successCounter.incrementAndGet();
            } finally {
                // 커넥션 종료
                simpleDb.closeConnection();
                // 작업이 완료되면 래치 카운터를 감소시킵니다.
                latch.countDown();
            }
        };

        //When
        // 쓰레드 풀에서 쓰레드를 할당받아 작업을 실행합니다.
        for (int i = 0; i < numberOfThreads; i++)
            executorService.submit(task);

        // 모든 작업이 완료될 때까지 대기하거나, 최대 10초 동안 대기합니다.
        latch.await(10, TimeUnit.SECONDS);

        // 쓰레드 풀을 종료시킵니다.
        executorService.shutdown();

        //Then
        // 성공 카운터가 쓰레드 수와 동일한지 확인합니다.
        assertThat(successCounter.get()).isEqualTo(numberOfThreads);
    }

    @Test
    @DisplayName("rollback")
    void t018() {
        //Given
        // SimpleDb에서 Sql 객체를 생성합니다.
        long oldCount = simpleDb.genSql()
                                .append("SELECT COUNT(*)")
                                .append("FROM article")
                                .selectLong();

        // 트랜잭션을 시작합니다.
        simpleDb.startTransaction();

        simpleDb.genSql()
                .append("INSERT INTO article")
                .append("(created_date, modified_date, title, body)")
                .append("VALUES (NOW(), NOW(), ?, ?)", "새 제목", "새 내용")
                .insert();

        simpleDb.rollback();

        //When
        long newCount = simpleDb.genSql()
                                .append("SELECT COUNT(*)")
                                .append("FROM article")
                                .selectLong();

        //Then
        assertThat(newCount).isEqualTo(oldCount);
    }

    @Test
    @DisplayName("commit")
    void t019() {
        //Given
        // SimpleDb에서 Sql 객체를 생성합니다.
        long oldCount = simpleDb.genSql()
                                .append("SELECT COUNT(*)")
                                .append("FROM article")
                                .selectLong();

        // 트랜잭션을 시작합니다.
        simpleDb.startTransaction();

        simpleDb.genSql()
                .append("INSERT INTO article")
                .append("(created_date, modified_date, title, body)")
                .append("VALUES (NOW(), NOW(), ?, ?)", "새 제목", "새 내용")
                .insert();

        simpleDb.commit();

        //When
        long newCount = simpleDb.genSql()
                                .append("SELECT COUNT(*)")
                                .append("FROM article")
                                .selectLong();

        //Then
        assertThat(newCount).isEqualTo(oldCount + 1);
    }

}