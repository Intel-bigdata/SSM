/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import AreachartVisualization from '../../../../visualization/builtins/storage-areachart';
angular.module('zeppelinWebApp').controller('StorageCtrl', StorageCtrl);
StorageCtrl.$inject = ['$scope', 'baseUrlSrv', '$filter', '$http', 'conf', '$interval'];
function StorageCtrl($scope, baseUrlSrv, $filter, $http, conf, $interval) {
  var tableData = {
    columns: [
      {name: "time", index: 0, aggr: "sum"},
      {name: "value(%)", index: 1, aggr: "sum"}
    ],
    rows:[],
    comment: ""
  };
  var config = {};

  var timeGranularity = 60;
  var timeRegular = 'HH:mm';

  var builtInViz;

  var getStorageData = function () {
    $http.get(baseUrlSrv.getSmartApiRoot() + conf.restapiProtocol + '/cluster/primary/hist_utilization/'
      +  $scope.storageType + '/' + timeGranularity + '000/-' + timeGranularity * 60 + '000/0')
      .then(function(response) {
        var storageData = angular.fromJson(response.data).body;
        var rows = new Array();
        angular.forEach(storageData, function (data, index) {
          rows.push([$filter('date')(new Date(data.timeStamp), timeRegular),
            (data.used / data.total * 100).toFixed(2)]);
        });
        tableData.rows = rows;
        initAreaChart();
    });
  };

  $scope.initStorage = function (storage) {
    $scope.storageType = storage;
    getStorageData();
  };

  $scope.selectTimeGranularity = function (time) {
    if (time === 0) {
      timeGranularity = 60;
      timeRegular = 'HH:mm'
    } else if (time === 1) {
      timeGranularity = 3600;
      timeRegular = 'dd HH:mm'
    } else if (time === 2) {
      timeGranularity = 3600 * 24;
      timeRegular = 'MM-dd'
    }
    getStorageData();
  };

  var initAreaChart = function() {
    var targetEl = angular.element('#' + $scope.storageType);
    //generate area chart.
    targetEl.height(300);
    if (!builtInViz) {
      builtInViz = new AreachartVisualization(targetEl, config);
      angular.element(window).resize(function () {
        builtInViz.resize();
      });
    }

    var transformation = builtInViz.getTransformation();
    var transformed = transformation.transform(tableData);
    builtInViz.render(transformed);
    builtInViz.activate();
  };

  var timer=$interval(function(){
    getStorageData();
  },30000);

  $scope.$on('$destroy',function(){
    $interval.cancel(timer);
  });
}
