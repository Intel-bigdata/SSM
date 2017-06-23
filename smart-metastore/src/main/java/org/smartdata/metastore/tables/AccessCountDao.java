package org.smartdata.metastore.tables;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

public class AccessCountDao {
  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public AccessCountDao(DataSource dataSource) {
    namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    jdbcTemplate = new JdbcTemplate(dataSource);
    simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("actions");
  }

  public void insert() {

  }

  public void delete() {

  }

  public void deleteAll() {

  }


}
