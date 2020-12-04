package org.smartdata.metastore.dao;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class WhitelistDao {
    private static final String TABLE_NAME = "whitelist";
    public static final String DIRS_FIELD = "last_fetched_dirs";
    private DataSource dataSource;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public WhitelistDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getLastFetchedDirs() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "SELECT * FROM " + TABLE_NAME;
        return jdbcTemplate.queryForObject(sql, String.class);
    }

    public void updateTable(String newWhitelist) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        final String sql = "UPDATE whitelist SET last_fetched_dirs =?";
        jdbcTemplate.update(sql, newWhitelist);
    }
}
