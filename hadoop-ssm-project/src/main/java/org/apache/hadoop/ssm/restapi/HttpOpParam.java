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
package org.apache.hadoop.ssm.restapi;


/** Http operation parameter. */
public abstract class HttpOpParam<E extends Enum<E> & HttpOpParam.Op>
    extends EnumParam<E> {
  /** Parameter name. */
  public static final String NAME = "op";

  /** Default parameter value. */
  public static final String DEFAULT = NULL;

  /** Http operation types */
  public enum Type {
    GET, PUT, POST, DELETE
  }

  /** Http operation interface. */
  public interface Op {
    /** @return the Http operation type. */
    Type getType();

    /** @return true if the operation cannot use a token */
    boolean getRequireAuth();

    /** @return true if the operation will do output. */
    boolean getDoOutput();

    /** @return true if the operation will be redirected. */
    boolean getRedirect();

    /** @return true the expected http response code. */
    int getExpectedHttpResponseCode();

    /** @return a URI query string. */
    String toQueryString();
  }

  /** @return the parameter value as a string */
  @Override
  public String getValueString() {
    return value.toString();
  }

  HttpOpParam(final Domain<E> domain, final E value) {
    super(domain, value);
  }
}
