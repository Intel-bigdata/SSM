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


export const ROUTES = [
    { path: '/app', title: 'Home', icon: 'account_balance', children: null },
    { path: 'rules', title: 'Rules', icon: 'short_text', children: null },
    { path: 'actions', title: 'Actions', icon: 'text_format', children: null },
    { path: '#component', id: 'component', title: 'Component', icon: 'apps', children: [
        {path: 'components/price-table', title: 'Price Table', icon: 'PT'},
        {path: 'components/panels', title: 'Panels', icon: 'P'},
        {path: 'components/wizard', title: 'Wizard', icon: 'W'},
      ]},
    { path: 'notification', title: 'Notification', icon: 'notifications', children: null },
    { path: 'alert', title: 'Sweet Alert', icon: 'warning', children: null },
    { path: 'settings', title: 'Settings', icon: 'settings', children: null },
];
