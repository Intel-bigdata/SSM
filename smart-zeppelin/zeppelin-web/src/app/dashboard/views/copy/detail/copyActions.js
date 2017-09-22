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

  .controller('CopyActionsCtrl', CopyActionsCtrl);
  CopyActionsCtrl.$inject = ['$scope', '$modal', '$sortableTableBuilder', '$dialogs', 'copyActions0'];
  function CopyActionsCtrl($scope, $modal, $stb, $dialogs, copyActions0) {

    $scope.actionsTable = {
      cols: [
        // group 1/3 (4-col)
        $stb.indicator().key('state').canSort('state.condition+"_"+createTime').styleClass('td-no-padding').done(),
        $stb.text('Cmdlet ID').key('cid').canSort().done(),
        $stb.text('File Path').key('filePath').canSort().styleClass('col-md-1').done(),
        $stb.text('Source Path').key('sourcePath').canSort().styleClass('col-md-1').done(),
        $stb.text('Target Path').key('targetPath').canSort().styleClass('col-md-1').done(),
        $stb.text('Status').key('succeed').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.duration("Running Time").key('runningTime').canSort().done(),
        $stb.datetime('Create Time').key('createTime').canSort().done(),
        $stb.datetime('Finish Time').key('finishTime').canSort().done(),
        $stb.progressbar('Progress').key('progress').sortBy('progress.usage').styleClass('col-md-1').done(),
        $stb.button('Actions').key(['view']).styleClass('col-md-1').done()
      ],
      rows: null
    };

    function updateTable(actions) {
      $scope.actionsTable.rows = $stb.$update($scope.actionsTable.rows,
        _.map(actions, function (action) {
          return {
            id: action.actionId,
            cid: action.cmdletId,
            state: {tooltip: action.status, condition: action.finished ? '' : 'good', shape: 'stripe'},
            createTime: action.createTime,
            finishTime: action.finished ? action.finishTime : "-",
            runningTime: action.uptime,
            succeed: action.finished ? action.successful : "-",
            view: {
              href: action.pageUrl,
              icon: function() {
                return 'glyphicon glyphicon-info-sign';
              },
              class: 'btn-xs btn-info'
            },
            progress: {
                current: action.progress,
                max: 1,
                flag: action.finished ? action.successful : "-"
                // usage: action.progress * 100
            },
            filePath: action.filePath,
            sourcePath: action.src,
            targetPath: action.target
          };
        }));
    }

    updateTable(copyActions0.$data());
    copyActions0.$subscribe($scope, function (actions) {
      updateTable(actions);
    });
  }

