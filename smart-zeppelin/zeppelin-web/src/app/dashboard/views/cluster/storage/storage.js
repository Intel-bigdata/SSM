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

import PiechartVisualization from '../../../../visualization/builtins/visualization-piechart';
angular.module('zeppelinWebApp').controller('StorageCtrl', StorageCtrl);
StorageCtrl.$inject = ['$scope', 'cache', 'ssd', 'disk', 'archive'];
function StorageCtrl($scope, cache, ssd, disk, archive) {
  var targetEls = new Array();
  var tableData = {
    columns: [
      {name: "usage", index: 0, aggr: "sum"},
      {name: "value", index: 1, aggr: "sum"}
    ],
    comment: ""
  };
  var config = {};

  var getGB = function (value) {
    var result = (value / (1024 * 1024 * 1024)).toFixed(2);
    return result;
  }

  var initPieChart = function(targetEl, data) {
    if (data.total === 0) {
      targetEl.remove();
      return;
    }
    //get pie chart data.
    var rows = [];
    rows[0] = ['Used(G)', getGB(data.used)];
    rows[1] = ['Free(G)', getGB(data.total - data.used)];
    tableData.rows = rows;

    //generate pie chart.
    targetEl.height(536);
    targetEls.push(targetEl);
    var builtInViz = new PiechartVisualization(targetEl, config);
    var transformation = builtInViz.getTransformation();
    var transformed = transformation.transform(tableData);
    builtInViz.render(transformed);
    builtInViz.donut();
    builtInViz.activate();
    angular.element(window).resize(function () {
      builtInViz.resize();
    });
  };

  initPieChart(angular.element('#cache'), cache.body);
  initPieChart(angular.element('#ssd'), ssd.body);
  initPieChart(angular.element('#disk'), disk.body);
  initPieChart(angular.element('#archive'), archive.body);

}
