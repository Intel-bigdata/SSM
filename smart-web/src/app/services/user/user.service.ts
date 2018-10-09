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

import { CookieService } from 'ngx-cookie-service';
import { HttpService } from '../http/http.service';

import { User } from '../../entity/User';

import { Observable } from 'rxjs';
import {catchError, tap} from "rxjs/operators";

@Injectable()
export class UserService {

  public user: User;

  constructor(
    private httpService: HttpService,
    private cookieService: CookieService ) { }

  /**
   * set user info.
   * @param principal - a `string` of user principal
   * @param ticket - a `string` of user ticket
   */
  setUser(principal: string, ticket: string): void {
    //TODO add roles.
    this.user = new User(principal, [], ticket);
  }

  /**
   * get the login status of user.
   * @return a `boolean` of login status
   */
  checkLogged(): boolean {
    if (this.cookieService.check('SSM_TICKET')) {
      const userTicket = JSON.parse(this.cookieService.get('SSM_TICKET'));
      this.setUser(userTicket.principal, userTicket.ticket);
    }
    return this.user !== undefined;
  }

  /**
   * user login.
   * @param userName - a `string` of user name
   * @param password - a `string` of user password
   * @return a `Observable` of login res
   */
  userLogin(userName: string, password: string): Observable<any> {
    return this.httpService.userLogin(userName, password).pipe(
      tap(res => {
        if (res.status === "OK") {
          const userTicket = {
            principal: res.body.principal,
            ticket: res.body.ticket
          };
          this.setUser(userTicket.principal, userTicket.ticket);
          this.cookieService.set('SSM_TICKET', JSON.stringify(userTicket));
        }
      })
    );
  }

  /**
   * user logout.
   */
  userLogout(): void {
    this.user = undefined;
    this.cookieService.delete('SSM_TICKET');
    this.httpService.userLogout().subscribe( res => {
        window.location.href = '/login';
      }
    );
  }
}
