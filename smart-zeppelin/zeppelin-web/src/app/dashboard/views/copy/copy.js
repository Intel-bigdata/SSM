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
        $stb.text('ID').key('id').canSort().done(),
        $stb.text('Text').key(['ruleText']).done(),
        $stb.text('Running Progress').key('runningProgress').done(),
        $stb.text('Base Progress').key('baseProgress').done(),
        $stb.text('Checked Number').key('numChecked').canSort().styleClass('hidden-sm hidden-xs').done(),
        $stb.progressbar('Progress').key('progress').sortBy('progress.usage').styleClass('col-md-1').done(),
        $stb.text('Status').key('status').canSort().styleClass('col-md-1 hidden-sm hidden-xs').done(),
        $stb.button('Actions').key(['view']).styleClass('col-md-1').done()
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
            ruleText: copy.ruleText,
            runningProgress: copy.runningProgress,
            baseProgress: copy.baseProgress,
            numChecked: copy.numChecked,
            progress: {
              current: copy.runningProgress,
              max: 1
            },
            status: copy.state,
            view: {
              href: copy.pageUrl,
              icon: function() {
                return 'glyphicon glyphicon-info-sign';
              },
              class: 'btn-xs btn-info'
            }
          };
        }));
    }

    updateTable(copys0.$data());
    copys0.$subscribe($scope, function (copys) {
      updateTable(copys);
    });
  }

