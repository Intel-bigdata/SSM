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
 * This controller is used to obtain action. All nested views will read status from here.
 */
  .controller('ActionCtrl', ActionCtrl);
  ActionCtrl.$inject = ['$scope', 'action0', '$propertyTableBuilder'];
  function ActionCtrl($scope, action0, $ptb) {
    'use strict';

    $scope.action = action0.$data();
    action0.$subscribe($scope, function (action) {
      $scope.action = action;
    });

    $scope.actionSummary = [
        $ptb.text('ID').done(),
        $ptb.datetime('Create Time').done(),
        $ptb.datetime('Finish Time').done(),
        $ptb.duration("Running Time").done(),
        $ptb.text('Result').done(),
        $ptb.text('Log').done()
      /*
       $ptb.text('User').done(),
       $ptb.button('Quick Links').done()
       */
    ];

    $scope.$watch('action', function (action) {
      $ptb.$update($scope.actionSummary, [
          action.actionId,
          action.createTime,
          action.finished ? action.finishTime : "",
          action.uptime,
          action.result,
          action.log
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
  }
