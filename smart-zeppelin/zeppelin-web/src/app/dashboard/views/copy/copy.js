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

  .controller('CopyCtrl', CopyCtrl);
CopyCtrl.$inject = ['$scope', '$modal', '$sortableTableBuilder', '$dialogs', 'copys0'];
  function CopyCtrl($scope, $modal, $stb, $dialogs, copys0) {

    $scope.copysTable = {
      cols: [
        // group 1/3 (4-col)
        $stb.indicator().key('state').canSort('state.condition+"_"+submitTime').styleClass('td-no-padding').done(),
        $stb.text('Rule ID').key('id').canSort().done(),
        $stb.text('Sync Rule').key(['syncRule']).done(),
        $stb.text('Syncing Files').key('syncingFiles').done(),
        $stb.text('All Files').key('allFiles').done(),
        $stb.progressbar('Progress').key('progress').sortBy('progress.usage').styleClass('col-md-1').done(),
        $stb.text('Status').key('status').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.button('Actions').key(['active', 'view', 'delete']).styleClass('col-md-1').done()
      ],
      rows: null
    };

    function updateTable(copys) {
      $scope.copysTable.rows = $stb.$update($scope.copysTable.rows,
        _.map(copys, function (copy) {
          return {
            id: copy.id,
            // name: {href: pageUrl, text: rule.appName},
            state: {tooltip: copy.state, condition: copy.isRunning ? 'good' : '', shape: 'stripe'},
            //user: rule.user,
            syncRule: copy.ruleText,
            syncingFiles: copy.runningProgress,
            allFiles: copy.baseProgress,
            progress: {
              current: copy.progress,
              max: 1
            },
            status: copy.state,
            active: {
              icon: function() {
                if(copy.isRunning) {
                  return 'glyphicon glyphicon-pause';
                }else {
                  return 'glyphicon glyphicon-play';
                }
              },
              class: 'btn-xs',
              disabled: copy.isDelete,
              click: function () {
                if(!copy.isRunning) {
                  copy.start();
                }else{
                  copy.terminate();
                }
              }
            },
            view: {
              href: copy.pageUrl,
              icon: function() {
                return 'glyphicon glyphicon-info-sign';
              },
              class: 'btn-xs btn-info'
            },
            delete: {
              icon: function() {
                return 'glyphicon glyphicon-trash';
              },
              class: 'btn-xs btn-danger',
              disabled: copy.isDelete,
              click: function () {
                $dialogs.confirm('Are you sure to delete this rule?', function () {
                  copy.delete();
                });
              }
            }
          };
        }));
    }

    updateTable(copys0.$data());
    copys0.$subscribe($scope, function (copys) {
      updateTable(copys);
    });
  }

