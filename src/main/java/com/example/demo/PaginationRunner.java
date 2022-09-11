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

  @Value("${pagination_runner.page_size:10}")
  private int pageSize;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Override
  public void run(ApplicationArguments args) throws Exception {
    logger.info("Starting PaginationRunner");
    loopThroughThePages();
    logger.info("Finished PaginationRunner");
  }

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

  private Page<Map<String, Object>> findAll(Pageable pageable) {
    long startId = pageable.getOffset();
    long endId = startId + pageable.getPageSize();
    String sql = String.format("SELECT * FROM word WHERE id > %s AND id <= %s", startId, endId);
    logger.info("findAll sql: {}", sql);
    List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
    long total = countAll();
    return new PageImpl<>(rows, pageable, total);
  }

  private long countAll() {
    String sql = "SELECT COUNT(*) FROM word";
    logger.info("countAll sql: {}", sql);
    return jdbcTemplate.queryForObject(sql, Long.class);
  }

  private void logProgress(Pageable pageable, Page<Map<String, Object>> page) {
    int currentPage = pageable.getPageNumber() + 1;
    int totalPages = page.getTotalPages();
    int currentRowCount = page.getNumberOfElements();
    long totalRowCount = page.getTotalElements();
    logger.info("On page {} of {}. Rows in this page: {}. Total rows: {}", currentPage, totalPages, currentRowCount, totalRowCount);
  }

  private void handleRow(Map<String, Object> row) {
    logger.info(row.toString());
  }

}
