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
angular.module('dashboard')

  .config(['$stateProvider',
    function ($stateProvider) {
      'use strict';

      $stateProvider
        .state('rules', {
          url: '/rules',
          templateUrl: 'views/rules/rules.html',
          controller: 'RulesCtrl',
          resolve: {
            rules0: ['models', function (models) {
              return models.$get.rules();
            }]
          }
        });
    }])

  .controller('RulesCtrl', ['$scope', '$modal', '$state', '$sortableTableBuilder', '$dialogs', 'rules0',
    function ($scope, $modal, $state, $stb, $dialogs, rules0) {
      'use strict';

      var submitWindow = $modal({
        templateUrl: 'views/rules/submit/submit.html',
        controller: 'RuleSubmitCtrl',
        backdrop: 'static',
        keyboard: true,
        show: false
      });

      $scope.openSubmitRuleDialog = function () {
        submitWindow.$promise.then(submitWindow.show);
      };

      $scope.composeMenuOptions = [{
        text: '<i class="glyphicon glyphicon-none"></i> <b>Submit Rule...</b>',
        click: $scope.openSubmitRuleDialog
      }];

      $scope.rulesTable = {
        cols: [
          // group 1/3 (4-col)
          $stb.indicator().key('state').canSort('state.condition+"_"+submitTime').styleClass('td-no-padding').done(),
          $stb.text('ID').key('id').canSort().done(),
          // $stb.link('Name').key('name').canSort('name.text').styleClass('col-md-1').done(),
          // group 2/3 (5-col)
          $stb.datetime('Submission Time').key('submitTime').canSort().sortDefaultDescent().done(),
          $stb.datetime('Last Check Time').key('lastCheckTime').canSort().sortDefaultDescent().done(),
          $stb.text('Checked Number').key('numChecked').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          $stb.text('Commands Generated').key('numCmdsGen').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          // $stb.datetime('Start Time').key('startTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          // $stb.datetime('Stop Time').key('stopTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          // $stb.text('User').key('user').canSort().styleClass('col-md-2').done(),
          // group 3/3 (4-col)
          $stb.text('Status').key('status').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
          $stb.button('Actions').key(['active', 'view', 'stop']).styleClass('col-md-3').done()
        ],
        rows: null
      };

      function updateTable(rules) {
        $scope.rulesTable.rows = $stb.$update($scope.rulesTable.rows,
          _.map(rules, function (rule) {
            return {
              id: rule.id,
              // name: {href: pageUrl, text: rule.appName},
              state: {tooltip: rule.state, condition: rule.isRunning ? 'good' : '', shape: 'stripe'},
              //user: rule.user,
              submitTime: rule.submitTime,
              lastCheckTime: rule.lastCheckTime,
              numChecked: rule.numChecked,
              numCmdsGen: rule.numCmdsGen,
              // startTime: rule.startTime,
              // stopTime: rule.finishTime || '-',
              status: rule.state,
              active: {
                text: 'Start', class: 'btn-xs', disabled: rule.isRunning,
                click: function () {
                  $dialogs.confirm('Are you sure to active this rule?', function () {
                    rule.start();
                  });
                }
              },
              view: {
                // href: rule.pageUrl,
                text: 'Details',
                class: 'btn-xs btn-primary',
                disabled: !rule.isRunning
              },
              stop: {
                text: 'Stop', class: 'btn-xs', disabled: !rule.isRunning,
                click: function () {
                  $dialogs.confirm('Are you sure to stop this rule?', function () {
                    rule.terminate();
                  });
                }
              }
            };
          }));
      }

      updateTable(rules0.$data());
      rules0.$subscribe($scope, function (rules) {
        updateTable(rules);
      });
    }])
;
