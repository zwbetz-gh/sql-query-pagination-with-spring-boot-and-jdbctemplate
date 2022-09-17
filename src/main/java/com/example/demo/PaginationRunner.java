package com.example.demo;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaginationRunner implements ApplicationRunner {

  Logger logger = LoggerFactory.getLogger(PaginationRunner.class);

  /**
   * The pageSize is configurable. We default it to 5 here.
   * You can override it in the src/main/resources/application.properties file by setting pagination_runner.page_size.
   * Or, via env var by setting PAGINATION_RUNNER_PAGE_SIZE.
   */
  @Value("${pagination_runner.page_size:5}")
  private int pageSize;

  /**
   * The jdbcTemplate uses the default data source. Which, in this demo, is the in-memory H2 database.
   */
  @Autowired
  private JdbcTemplate jdbcTemplate;

  /**
   * This class implements ApplicationRunner.
   * So, this component will run after the Spring Application Context is initialized.
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {
    logger.info("Starting PaginationRunner");
    loopThroughThePages();
    logger.info("Finished PaginationRunner");
  }

  /**
   * Loop through the pages until you encounter an empty page.
   */
  private void loopThroughThePages() {
    Pageable pageable = PageRequest.of(0, pageSize);
    Page<Map<String, Object>> page = findAll(pageable);

    while (!page.isEmpty()) {
      logProgress(pageable, page);
      page.stream().forEach(this::handleRow);
      pageable = pageable.next();
      page = findAll(pageable);
    }
  }

  /**
   * Find all the rows.
   * You _could_ create the query using LIMIT and OFFSET...
   * But, I went with a plain WHERE clause that selects a range of IDs because it's faster.
   */
  private Page<Map<String, Object>> findAll(Pageable pageable) {
    long startId = pageable.getOffset();
    long endId = startId + pageable.getPageSize();
    String sql = String.format(
        "SELECT * FROM word WHERE id > %s AND id <= %s",
        startId,
        endId
    );
    logger.info("findAll sql: {}", sql);
    List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
    long total = countAll();
    return new PageImpl<>(rows, pageable, total);
  }

  /**
   * Count all the rows.
   */
  private long countAll() {
    String sql = "SELECT COUNT(*) FROM word";
    logger.info("countAll sql: {}", sql);
    return jdbcTemplate.queryForObject(sql, Long.class);
  }

  /**
   * Log the progress.
   * You'll thank yourself for this, especially if the "job" is long-running.
   */
  private void logProgress(Pageable pageable, Page<Map<String, Object>> page) {
    int currentPage = pageable.getPageNumber() + 1;
    int totalPages = page.getTotalPages();
    int currentRowCount = page.getNumberOfElements();
    long totalRowCount = page.getTotalElements();
    logger.info(
        "On page {} of {}. Rows in page: {}. Total rows: {}",
        currentPage, totalPages, currentRowCount, totalRowCount
    );
  }

  /**
   * Actually do something with each row.
   * In this demo, I'm just logging the row.
   * In a real scenario, maybe you're building up a bulk request to send somewhere else, etc.
   */
  private void handleRow(Map<String, Object> row) {
    logger.info(row.toString());
  }

}
