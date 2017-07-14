/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
angular.module('zeppelinWebApp')

  .directive('cmdletsTable', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'app/dashboard/views/rules/rule/cmdlets_table.html',
      replace: false /* true will got an error */,
      scope: {
        cmdlets: '=cmdletsBind'
      },
      controller: ['$scope', '$sortableTableBuilder', 'i18n',
        function ($scope, $stb, i18n) {
          $scope.whatIsCmdlet = i18n.terminology.cmdlet;
          $scope.table = {
            cols: [
              $stb.indicator().key('status').canSort().styleClass('td-no-padding').done(),
              $stb.link('Id').key('id').canSort().sortDefault().styleClass('col-xs-2').done(),
              $stb.text('Arguments').key('arguments').canSort().styleClass('col-xs-6').done(),
              $stb.datetime('Generate Time').key('generateTime').canSort().styleClass('col-xs-4').done()
            ],
            rows: null
          };

          function updateTable(cmdlets) {
            $scope.table.rows = $stb.$update($scope.table.rows,
              _.map(cmdlets, function (cmdlet) {
                return {
                  arguments: cmdlet.parameters,
                  generateTime: cmdlet.generateTime,
                  status: {
                    tooltip: cmdlet.state,
                    condition: cmdlet.isRunning ? 'good' : '',
                    shape: 'stripe'
                  },
                  id: {
                    href: cmdlet.pageUrl, text: cmdlet.cid
                  }
                };
              }));
          }

          $scope.$watch('cmdlets', function (cmdlets) {
            updateTable(cmdlets);
          });
        }]
    };
  })
;
