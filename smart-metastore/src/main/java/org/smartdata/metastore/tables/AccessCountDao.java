package org.smartdata.metastore.tables;

import org.smartdata.common.models.FileAccessInfo;
import org.smartdata.metastore.MetaStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class AccessCountDao {
  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  private FileDao fileDao;
  public final static String FILE_FIELD = "fid";
  public final static String ACCESSCOUNT_FIELD = "count";

  public AccessCountDao(DataSource dataSource) {
    namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    jdbcTemplate = new JdbcTemplate(dataSource);
    simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("access_count_tables");
    fileDao = new FileDao(dataSource);
  }

  public void insert() {

  }

  public void delete() {

  }

  public void deleteAll() {

  }

  public static String createTableSQL(String tableName) {
    return String.format(
        "CREATE TABLE %s (%s INTEGER NOT NULL, %s INTEGER NOT NULL)",
        tableName, FILE_FIELD, ACCESSCOUNT_FIELD);
  }

  public String aggregateSQLStatement(AccessCountTable destinationTable
      , List<AccessCountTable> tablesToAggregate) {
    StringBuilder statement = new StringBuilder();
    statement.append("CREATE TABLE '" + destinationTable.getTableName() + "' as ");
    statement.append("SELECT " + AccessCountDao.FILE_FIELD + ", SUM(" +
        AccessCountDao.ACCESSCOUNT_FIELD + ") as " +
        AccessCountDao.ACCESSCOUNT_FIELD + " FROM (");
    Iterator<AccessCountTable> tableIterator = tablesToAggregate.iterator();
    while (tableIterator.hasNext()) {
      AccessCountTable table = tableIterator.next();
      if (tableIterator.hasNext()) {
        statement.append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
      } else {
        statement.append("SELECT * FROM " + table.getTableName());
      }
    }
    statement.append(") tmp GROUP BY " + AccessCountDao.FILE_FIELD);
    return statement.toString();
  }

/*  public synchronized List<FileAccessInfo> getHotFiles(List<AccessCountTable> tables,
                                                       int topNum) throws SQLException {
    Iterator<AccessCountTable> tableIterator = tables.iterator();
    if (tableIterator.hasNext()) {
      StringBuilder unioned = new StringBuilder();
      while (tableIterator.hasNext()) {
        AccessCountTable table = tableIterator.next();
        if (tableIterator.hasNext()) {
          unioned.append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
        } else {
          unioned.append("SELECT * FROM " + table.getTableName());
        }
      }
      String statement =
          String.format(
              "SELECT %s, SUM(%s) as %s FROM (%s) tmp GROUP BY %s ORDER BY %s DESC LIMIT %s",
              AccessCountDao.FILE_FIELD,
              AccessCountDao.ACCESSCOUNT_FIELD,
              AccessCountDao.ACCESSCOUNT_FIELD,
              unioned,
              AccessCountDao.FILE_FIELD,
              AccessCountDao.ACCESSCOUNT_FIELD,
              topNum);
      ResultSet resultSet = this.executeQuery(statement);
      Map<Long, Integer> accessCounts = new HashMap<>();
      while (resultSet.next()) {
        accessCounts.put(
            resultSet.getLong(AccessCountDao.FILE_FIELD),
            resultSet.getInt(AccessCountDao.ACCESSCOUNT_FIELD));
      }
      Map<Long, String> idToPath = this.getFilePaths(accessCounts.keySet());
      List<FileAccessInfo> result = new ArrayList<>();
      for (Map.Entry<Long, Integer> entry : accessCounts.entrySet()) {
        Long fid = entry.getKey();
        if (idToPath.containsKey(fid)) {
          result.add(new FileAccessInfo(fid, idToPath.get(fid), accessCounts.get(fid)));
        }
      }
      return result;
    } else {
      return new ArrayList<>();
    }
  }*/

/*  public Map<Long, Integer> getAccessCount(long startTime, long endTime,
                                           String countFilter) throws SQLException {
    // TODO access file
    Map<Long, Integer> ret = new HashMap<>();
    String sqlGetTableNames = "SELECT table_name FROM access_count_tables "
        + "WHERE start_time >= " + startTime + " AND end_time <= " + endTime;
    Connection conn = getConnection();
    MetaStore.QueryHelper qhTableName = null;
    ResultSet rsTableNames = null;
    MetaStore.QueryHelper qhValues = null;
    ResultSet rsValues = null;
    try {
      qhTableName = new MetaStore.QueryHelper(sqlGetTableNames, conn);
      rsTableNames = qhTableName.executeQuery();
      List<String> tableNames = new LinkedList<>();
      while (rsTableNames.next()) {
        tableNames.add(rsTableNames.getString(1));
      }
      qhTableName.close();

      if (tableNames.size() == 0) {
        return ret;
      }

      String sqlPrefix = "SELECT fid, SUM(count) AS count FROM (\n";
      String sqlUnion = "SELECT fid, count FROM \'"
          + tableNames.get(0) + "\'\n";
      for (int i = 1; i < tableNames.size(); i++) {
        sqlUnion += "UNION ALL\n" +
            "SELECT fid, count FROM \'" + tableNames.get(i) + "\'\n";
      }
      String sqlSufix = ") GROUP BY fid ";
      // TODO: safe check
      String sqlCountFilter =
          (countFilter == null || countFilter.length() == 0) ?
              "" :
              "HAVING SUM(count) " + countFilter;
      String sqlFinal = sqlPrefix + sqlUnion + sqlSufix + sqlCountFilter;

      qhValues = new MetaStore.QueryHelper(sqlFinal, conn);
      rsValues = qhValues.executeQuery();

      while (rsValues.next()) {
        ret.put(rsValues.getLong(1), rsValues.getInt(2));
      }

      return ret;
    } finally {
      if (qhTableName != null) {
        qhTableName.close();
      }

      if (qhValues != null) {
        qhValues.close();
      }

      closeConnection(conn);
    }
  }*/

  public void createProportionView(AccessCountTable dest, AccessCountTable source)
      throws SQLException {
    double percentage =
        ((double) dest.getEndTime() - dest.getStartTime())
            / (source.getEndTime() - source.getStartTime());
    String sql =
        String.format(
            "CREATE VIEW %s AS SELECT %s, FLOOR(%s.%s * %s) AS %s FROM %s",
            dest.getTableName(),
            AccessCountDao.FILE_FIELD,
            source.getTableName(),
            AccessCountDao.ACCESSCOUNT_FIELD,
            percentage,
            AccessCountDao.ACCESSCOUNT_FIELD,
            source.getTableName());
    jdbcTemplate.execute(sql);
  }

}
