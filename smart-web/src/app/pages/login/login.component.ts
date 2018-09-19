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


import { Component, OnInit, ViewChild, ElementRef } from '@angular/core';
import { SwalComponent } from '@toverux/ngx-sweetalert2';

import { Router} from '@angular/router';
import { UserService } from '../../services/user/user.service';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent implements OnInit {

  @ViewChild('nameInput') nameInput: ElementRef;
  @ViewChild('loginErrorSwal') private loginErrorSwal: SwalComponent;
  loginName: string = "";
  password: string = "";

  constructor( private userService: UserService, private router: Router ) { }

  ngOnInit(): void {
    if (this.userService.checkLogged()) {
      // Redirect to app when user has logged.
      this.router.navigateByUrl('/');
    }
  }

  login(): void {
    if (this.loginName === "" || this.password === "") {
      this.loginErrorSwal.show();
      return;
    }

    console.log(this.loginName + this.password);

  }
}
