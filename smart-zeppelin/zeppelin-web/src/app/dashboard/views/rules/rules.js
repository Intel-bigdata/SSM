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
        $stb.indicator().key('state').canSort('state.condition+"_"+submitTime').styleClass('td-no-padding').done(),
        $stb.text('ID').key('id').canSort().sortDefaultDescent().done(),
        $stb.text('Rule Text').key(['ruleText']).styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.datetime('Submission Time').key('submitTime').canSort().done(),
        $stb.datetime('Last Check Time').key('lastCheckTime').canSort().done(),
        $stb.text('Checked Number').key('numChecked').canSort().styleClass('hidden-sm hidden-xs').done(),
        $stb.text('Cmdlets Generated').key('numCmdsGen').canSort().styleClass('hidden-sm hidden-xs').done(),
        $stb.text('Status').key('status').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.button('Actions').key(['active', 'view', 'delete']).styleClass('col-md-1').done()
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
            ruleText: {
              value: rule.ruleText.length > 70 ? rule.ruleText.substring(0, 70) + ' ...' : rule.ruleText,
              title: rule.ruleText
            },
            submitTime: rule.submitTime,
            lastCheckTime: rule.lastCheckTime === 0 ? '-' : rule.lastCheckTime,
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
                  rule.start();
                  rule.isRunning = true;
                }else{
                  rule.terminate();
                  rule.isRunning = false;
                }
              }
            },
            view: {
              href: rule.pageUrl,
              icon: function() {
                return 'glyphicon glyphicon-info-sign';
              },
              class: 'btn-xs btn-info'
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
