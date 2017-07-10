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

  // .config(['$stateProvider',
  //   function ($stateProvider) {
  //     'use strict';
  //
  //     $stateProvider
  //       .state('rule.overview', {
  //         url: '', /* default page */
  //         templateUrl: 'views/rules/rule/overview.html',
  //         controller: 'RuleOverviewCtrl'
  //       });
  //   }])

  .controller('RuleOverviewCtrl', ['$scope', 'helper', '$propertyTableBuilder', 'models',
    function ($scope, helper, $ptb, models) {
      'use strict';

      $scope.ruleSummary = [
        $ptb.text('ID').done(),
        $ptb.datetime('Start Time').done()
        /*
        $ptb.text('User').done(),
        $ptb.button('Quick Links').done()
        */
      ];

      $scope.$watch('rule', function (rule) {
        $ptb.$update($scope.ruleSummary, [
          rule.id,
          rule.submitTime
          /*
          rule.user,
          [
            {href: rule.configLink, target: '_blank', text: 'Config', class: 'btn-xs'},
            helper.withClickToCopy({text: 'Home Dir.', class: 'btn-xs'}, rule.homeDirectory),
            helper.withClickToCopy({text: 'Log Dir.', class: 'btn-xs'}, rule.logFile)
          ]
          */
        ]);
      });

      $scope.alerts = [];
      models.$get.ruleAlerts($scope.rule.id)
        .then(function (alerts0) {
          $scope.alerts = alerts0.$data();
          alerts0.$subscribe($scope, function (alerts) {
            $scope.alerts = alerts;
          });
        });

      $scope.cmdlets = [];
      models.$get.ruleCmdlets($scope.rule.id)
        .then(function (cmdlets0) {
          $scope.cmdlets = cmdlets0.$data();
          cmdlets0.$subscribe($scope, function (cmdlets) {
            $scope.cmdlets = cmdlets;
          });
      });
    }])
;
