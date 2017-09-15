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

  .controller('MoverCtrl', MoverCtrl);
  MoverCtrl.$inject = ['$scope', '$modal', '$sortableTableBuilder', '$dialogs', 'movers0'];
  function MoverCtrl($scope, $modal, $stb, $dialogs, movers0) {

    $scope.moversTable = {
      cols: [
        // group 1/3 (4-col)
        $stb.indicator().key('state').canSort('state.condition+"_"+submitTime').styleClass('td-no-padding').done(),
        $stb.text('Rule ID').key('id').canSort().done(),
        $stb.text('Mover Rule').key(['moverRule']).done(),
        $stb.text('Moving Files').key('movingFiles').done(),
        $stb.text('All Files').key('allFiles').done(),
        $stb.progressbar('Progress').key('progress').sortBy('progress.usage').styleClass('col-md-1').done(),
        $stb.text('Status').key('status').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.button('Actions').key(['active', 'view', 'delete']).styleClass('col-md-1').done()
      ],
      rows: null
    };

    function updateTable(movers) {
      $scope.moversTable.rows = $stb.$update($scope.moversTable.rows,
        _.map(movers, function (mover) {
          return {
            id: mover.id,
            // name: {href: pageUrl, text: rule.appName},
            state: {tooltip: mover.state, condition: mover.isRunning ? 'good' : '', shape: 'stripe'},
            //user: rule.user,
            moverRule: mover.ruleText,
            movingFiles: mover.runningProgress,
            allFiles: mover.baseProgress,
            progress: {
              current: mover.progress,
              max: 1
            },
            status: mover.state,
            active: {
              icon: function() {
                if(mover.isRunning) {
                  return 'glyphicon glyphicon-pause';
                }else {
                  return 'glyphicon glyphicon-play';
                }
              },
              class: 'btn-xs',
              disabled: mover.isDelete,
              click: function () {
                if(!mover.isRunning) {
                  mover.start();
                }else{
                  mover.terminate();
                }
              }
            },
            view: {
              href: mover.pageUrl,
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
              disabled: mover.isDelete,
              click: function () {
                $dialogs.confirm('Are you sure to delete this rule?', function () {
                  mover.delete();
                });
              }
            }
          };
        }));
    }

    updateTable(movers0.$data());
    movers0.$subscribe($scope, function (movers) {
      updateTable(movers);
    });
  }

