/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.metastore.dao;

import org.smartdata.model.SmartFileCompressionInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.reflect.*;

/**
 * CompressionFileDao.
 */
public class CompressionFileDao {
  private String TABLE_NAME = "compression_file";

  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public CompressionFileDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void insert(SmartFileCompressionInfo compressionInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.execute(toMap(compressionInfo));
  }
  
  public void deleteByName(String fileName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE file_name = ?";
    jdbcTemplate.update(sql, fileName);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  public List<SmartFileCompressionInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME,
        new CompressFileRowMapper());
  }

  public SmartFileCompressionInfo getInfoByName(String fileName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM " + TABLE_NAME + " WHERE file_name = ?",
        new Object[]{fileName}, new CompressFileRowMapper());
  }

  private Map<String, Object> toMap(SmartFileCompressionInfo compressionInfo) {
    Gson gson = new Gson();
    Map<String, Object> parameters = new HashMap<>();
    Long[] originalPos = compressionInfo.getOriginalPos();
    Long[] compressedPos = compressionInfo.getCompressedPos();
    String originalPosGson = gson.toJson(originalPos);
    String compressedPosGson = gson.toJson(compressedPos);
    parameters.put("file_name", compressionInfo.getFileName());
    parameters.put("buffer_size", compressionInfo.getBufferSize());
    parameters.put("original_length", compressionInfo.getOriginalLength());
    parameters.put("compressed_length", compressionInfo.getCompressedLength());
    parameters.put("originalPos",originalPosGson);
    parameters.put("compressedPos",compressedPosGson);
    return parameters;
  }

  class CompressFileRowMapper implements RowMapper<SmartFileCompressionInfo> {
    @Override
    public SmartFileCompressionInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      Gson gson = new Gson();
      String originalPosGson = resultSet.getString("originalPos");
      String compressedPosGson = resultSet.getString("compressedPos");
      Long[] originalPos = gson.fromJson(originalPosGson,new TypeToken<Long[]>(){}.getType());
      Long[] compressedPos = gson.fromJson(compressedPosGson,new TypeToken<Long[]>(){}.getType());
      SmartFileCompressionInfo compressionInfo =
          SmartFileCompressionInfo.newBuilder()
          .setFileName(resultSet.getString("file_name"))
          .setBufferSize(resultSet.getInt("buffer_size"))
          .setOriginalLength(resultSet.getLong("original_length"))
          .setCompressedLength(resultSet.getLong("compressed_length"))
          .setOriginalPos(originalPos)
          .setCompressedPos(compressedPos)
          .build();
      return compressionInfo;
    }
  }
}