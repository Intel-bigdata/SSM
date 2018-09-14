/**
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


import {Component, OnInit, ViewChild, ElementRef} from '@angular/core';
import {FormControl, Validators} from '@angular/forms';
import { SwalComponent } from '@toverux/ngx-sweetalert2';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent implements OnInit {

  @ViewChild('nameInput') nameInput: ElementRef;
  @ViewChild('deleteSwal') private deleteSwal: SwalComponent;
  loginName: string = "";
  password: string = "";

  loginFormControl = new FormControl('', [
    Validators.required
  ]);

  ngOnInit() {

  }

  login(): void {
    if (this.loginName === "" || this.password === "") {
      this.deleteSwal.show();
      return;
    }

    console.log(this.loginName + this.password);

  }
}
