/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environment';

@Injectable()
export class BaseUrlService {

  constructor() { }

  /**
   *  get smart storage api root
   *  @return a `string` of smart api root
   */
  getRestApiRoot(): string {
    return environment.production ? '/smart/api/' : 'http://localhost:7045/smart/api/'
  };

  /**
   * get user api root in zeppelin api
   * @return a `string` of user api root
   */
  getUserApiRoot(): string {
    return environment.production ? '/api/' : 'http://localhost:7045/api/'
  };
}
