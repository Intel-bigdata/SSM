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

/**
 * This controller is used to obtain rule. All nested views will read status from here.
 */
  .controller('RuleCtrl', RuleCtrl);
  RuleCtrl.$inject = ['$scope', 'rule0', '$propertyTableBuilder', 'models', '$interval',
    'baseUrlSrv', '$filter', '$http', 'conf', '$route' ];
  function RuleCtrl($scope, rule0, $ptb, models, $interval,
                    baseUrlSrv, $filter, $http, conf, $route) {
    'use strict';

    $scope.pageNumber = 5;
    $scope.totalNumber = 0;
    $scope.cmdlets;
    $scope.currentPage = 1;
    $scope.totalPage = 1;
    $scope.orderby = 'cid';
    $scope.isDesc = true;

    var ruleId = $route.current.params.ruleId;
    $scope.rule = rule0.$data();
    rule0.$subscribe($scope, function (rule) {
      $scope.rule = rule;
    });

    $scope.ruleSummary = [
        $ptb.text('ID').done(),
        $ptb.datetime('Start Time').done()
    ];

    $scope.$watch('rule', function (rule) {
      $ptb.$update($scope.ruleSummary, [
          rule.id,
          rule.submitTime
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

    function getCmdlets() {
      $http.get(baseUrlSrv.getSmartApiRoot() + conf.restapiProtocol + '/rules/' + ruleId + '/cmdlets/'
        + $scope.currentPage + '/' + $scope.pageNumber + '/' + $scope.orderby + '/' + $scope.isDesc)
        .then(function(response) {
          var cmdletsData = angular.fromJson(response.data);
          $scope.totalNumber = cmdletsData.body.totalNumOfCmdlets;
          $scope.cmdlets = cmdletsData.body.cmdlets;
          angular.forEach($scope.cmdlets, function (data,index) {
            data.generateTime = data.generateTime === 0 ? "-" :
              $filter('date')(data.generateTime,'yyyy-MM-dd HH:mm:ss');
            data.stateColor = data.state === "DONE" ? "green" : data.state === "FAILED" ? "red" : "gray";
          });
          $scope.totalPage = Math.ceil($scope.totalNumber / $scope.pageNumber);
        }, function(errorResponse) {
          $scope.totalNumber = 0;
        });
    };

    $scope.gotoPage = function (index) {
      $scope.currentPage = index;
      getCmdlets();
    };
    $scope.defindOrderBy = function (filed) {
      if ($scope.orderby === filed) {
        $scope.isDesc = ! $scope.isDesc;
      } else {
        $scope.orderby = filed;
        $scope.isDesc = true;
      }
      getCmdlets();
    };
    getCmdlets();

    var timer=$interval(function(){
      getCmdlets();
    },5000);

    $scope.$on('$destroy',function(){
      $interval.cancel(timer);
    });
  }
