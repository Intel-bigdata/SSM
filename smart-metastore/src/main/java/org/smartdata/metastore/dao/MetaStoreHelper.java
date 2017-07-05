package org.smartdata.metastore.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MetaStoreHelper {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public MetaStoreHelper(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void execute(String sql) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.execute(sql);
  }

  public void dropTable(String tableName) {
    String sql = "DROP TABLE IF EXISTS " + tableName;
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.execute(sql);
  }

  public void dropView(String viewName) {
    String sql = "DROP VIEW IF EXISTS " + viewName;
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.execute(sql);
  }

  public List<String> getFilesPath(String sql) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(sql, new ResultSetExtractor<List<String>>() {
      public List<String> extractData(ResultSet rs) throws SQLException {
        List<String> files = new ArrayList<>();
        while(rs.next()) {
          files.add(rs.getString(1));
        }
        return files;
      }
    });
  }
}
