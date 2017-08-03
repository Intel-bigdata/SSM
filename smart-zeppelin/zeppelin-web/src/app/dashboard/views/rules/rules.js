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

  .controller('RulesCtrl', RulesCtrl);

  RulesCtrl.$inject = ['$scope', '$modal', '$sortableTableBuilder', '$dialogs', 'rules0'];

  function RulesCtrl ($scope, $modal, $stb, $dialogs, rules0) {
    'use strict';

    var submitWindow = $modal({
      templateUrl: 'app/dashboard/views/rules/submit/submit.html',
      controller: 'RuleSubmitCtrl',
      backdrop: 'static',
      keyboard: true,
      show: false
    });

    $scope.openSubmitRuleDialog = function () {
      submitWindow.$promise.then(submitWindow.show);
    };

    $scope.rulesTable = {
      cols: [
        // group 1/3 (4-col)
        $stb.indicator().key('state').canSort('state.condition+"_"+submitTime').styleClass('td-no-padding').done(),
        $stb.text('ID').key('id').canSort().sortDefaultDescent().done(),
        $stb.text('Name').key(['ruleName']).canSort().done(),
          // $stb.link('Name').key('name').canSort('name.text').styleClass('col-md-1').done(),
        // group 2/3 (5-col)
        $stb.datetime('Submission Time').key('submitTime').canSort().done(),
        $stb.datetime('Last Check Time').key('lastCheckTime').canSort().done(),
        $stb.text('Checked Number').key('numChecked').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.text('Cmdlets Generated').key('numCmdsGen').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        // $stb.datetime('Start Time').key('startTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        // $stb.datetime('Stop Time').key('stopTime').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        // $stb.text('User').key('user').canSort().styleClass('col-md-2').done(),
        // group 3/3 (4-col)
        $stb.text('Status').key('status').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.button('Actions').key(['active', 'view', 'delete']).styleClass('col-md-4').done()
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
            ruleName: {
              value: rule.ruleName,
              title: "ID:" + rule.id + " Name:" + rule.ruleName
              + " Submission Time:" + new Date(rule.submitTime).toUTCString()
              + " Last Check Time:" + new Date(rule.lastCheckTime).toUTCString()
              + " Checked Number:" + rule.numChecked
              + " Cmdlets Generated:" + rule.numCmdsGen
              + " Status:" +  rule.state
            },
            submitTime: rule.submitTime,
            lastCheckTime: rule.lastCheckTime,
            numChecked: rule.numChecked,
            numCmdsGen: rule.numCmdsGen,
            // startTime: rule.startTime,
            // stopTime: rule.finishTime || '-',
            status: rule.state,
            active: {
              icon: function() {
                if(rule.isRunning) {
                  return 'glyphicon glyphicon-pause';
                }else {
                  return 'glyphicon glyphicon-play';
                }
              },
              class: 'btn-xs',
              disabled: rule.isDelete,
              click: function () {
                if(!rule.isRunning) {
                  $dialogs.confirm('Are you sure to active this rule?', function () {
                    rule.start();
                  });
                }else{
                  $dialogs.confirm('Are you sure to stop this rule?', function () {
                    rule.terminate();
                  });
                }
              }
            },
            view: {
              href: rule.pageUrl,
              icon: function() {
                return 'glyphicon glyphicon-info-sign';
              },
              class: 'btn-xs btn-info',
              disabled: !rule.isRunning
            },
            // stop: {
            //   text: 'glyphicon glyphicon-stop',
            //   class: 'btn-xs btn-warning', disabled: !rule.isRunning,
            //   click: function () {
            //     $dialogs.confirm('Are you sure to stop this rule?', function () {
            //       rule.terminate();
            //     });
            //   }
            // },
            delete: {
              icon: function() {
                return 'glyphicon glyphicon-trash';
              },
              class: 'btn-xs btn-danger',
              disabled: rule.isDelete,
              click: function () {
                $dialogs.confirm('Are you sure to delete this rule?', function () {
                  rule.delete();
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
  }
