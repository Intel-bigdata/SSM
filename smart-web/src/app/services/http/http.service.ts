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
import { BaseUrlService } from '../baseUrl/baseUrl.service';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs';
import { catchError, tap } from "rxjs/operators";
import { of } from 'rxjs/observable/of';

@Injectable()
export class HttpService {

  constructor(
    private http: HttpClient,
    private baseUrlService: BaseUrlService
  ) { }

  /**
   * POST: user login.
   * @param userName - a `string` of user name
   * @param password - a `string` of user password
   */
  userLogin(userName: string, password: string): Observable<any> {
    const httpOptions = {
      headers: new HttpHeaders({ 'Content-Type': 'application/x-www-form-urlencoded' })
    };
    const loginUrl = `${ this.baseUrlService.getUserApiRoot() }login`;
    // format user name and password to form data.
    let params: string = `userName=${ userName }&password=${ password }`;
    return this.http.post(loginUrl, params, httpOptions);
  }

  /**
   * Handle Http operation that failed.
   * Let the app continue.
   * @param operation - name of the operation that failed
   * @param result - optional value to return as the observable result
   */
  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {

      // TODO: send the error to remote logging infrastructure
      console.error(operation, error); // log to console instead

      // Let the app keep running by returning an empty result.
      // return setting result or http error text.
      return of( result ? result : error.error as T);
    };
  }
}
