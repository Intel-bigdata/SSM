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

  .controller('ActionsCtrl', ActionsCtrl);
  ActionsCtrl.$inject = ['$scope', '$modal', '$sortableTableBuilder',
    '$dialogs', 'actions0', 'actionTypes'];
  function ActionsCtrl($scope, $modal, $stb, $dialogs, actions0, actionTypes) {
    'use strict';

    var submitWindow = $modal({
      templateUrl: 'app/dashboard/views/actions/submit/submit.html',
      controller: 'ActionSubmitCtrl',
      backdrop: 'static',
      keyboard: true,
      show: false,
      resolve: {
        actionTypes: function () {
          return actionTypes;
        }
      }
    });

    $scope.openSubmitActionDialog = function () {
      submitWindow.$promise.then(submitWindow.show);
    };

    $scope.actionsTable = {
      cols: [
        // group 1/3 (4-col)
        $stb.indicator().key('state').canSort('state.condition+"_"+createTime').styleClass('td-no-padding').done(),
        $stb.text('ID').key('id').canSort().sortDefaultDescent().done(),
        $stb.text('Cmdlet ID').key('cid').canSort().done(),
        $stb.text('Name').key('actionName').canSort().done(),
          // $stb.link('Name').key('name').canSort('name.text').styleClass('col-md-1').done(),
        // group 2/3 (5-col)
        $stb.datetime('Create Time').key('createTime').canSort().done(),
        $stb.datetime('Finish Time').key('finishTime').canSort().done(),
        $stb.duration("Running Time").key('runningTime').canSort().done(),

          // $stb.datetime('Start Time').key('startTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        // $stb.datetime('Stop Time').key('stopTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        // $stb.text('User').key('user').canSort().styleClass('col-md-2').done(),
        // group 3/3 (4-col)
        $stb.text('Succeed').key('succeed').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.progressbar('Progress').key('progress').sortBy('progress.usage').styleClass('col-md-1').done(),
        $stb.button('Actions').key(['view']).styleClass('col-md-3').done()
      ],
      rows: null
    };

    function updateTable(actions) {
      $scope.actionsTable.rows = $stb.$update($scope.actionsTable.rows,
        _.map(actions, function (action) {
          return {
            id: action.actionId,
            cid: action.cmdletId,
            // name: {href: pageUrl, text: rule.appName},
            state: {tooltip: action.status, condition: action.finished ? '' : 'good', shape: 'stripe'},
            //user: rule.user,
            actionName: action.actionName,
            createTime: action.createTime,
            finishTime: action.finished ? action.finishTime : "-",
            runningTime: action.uptime,
            // startTime: rule.startTime,
            // stopTime: rule.finishTime || '-',
            succeed: action.finished ? action.successful : "-",
            view: {
              href: action.pageUrl,
              text: 'Show Result',
              class: 'btn-xs btn-primary'
            },
            progress: {
                current: action.progress,
                max: 1,
                usage: action.progress * 100
            }
          };
        }));
    }

    updateTable(actions0.$data());
    actions0.$subscribe($scope, function (actions) {
      updateTable(actions);
    });
  }

