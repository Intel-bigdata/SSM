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

  .controller('MoverActionsCtrl', MoverActionsCtrl);
MoverActionsCtrl.$inject = ['$scope', 'baseUrlSrv', '$filter', '$http', 'conf', '$route', '$interval'];
function MoverActionsCtrl($scope, baseUrlSrv, $filter, $http, conf, $route, $interval) {
  $scope.pageNumber = 10;
  $scope.totalNumber = 0;
  $scope.copyActions;
  $scope.currentPage = 1;
  $scope.totalPage = 1;
  $scope.orderby = 'aid';
  $scope.isDesc = true;
  var ruleId = $route.current.params.ruleId;
  function getCopyActions() {
    $http.get(baseUrlSrv.getSmartApiRoot() + conf.restapiProtocol + '/actions/filelist/'
      + ruleId + '/' + $scope.currentPage + '/' + $scope.pageNumber)
      .then(function(response) {
        var actionData = angular.fromJson(response.data);
        $scope.totalNumber = actionData.body.totalNumOfActions;
        $scope.copyActions = actionData.body.detailedFileActions;
        angular.forEach($scope.copyActions, function (data,index) {
          data.runTime = data.finishTime - data.createTime;
          data.createTime = data.createTime === 0 ? "-" :
            $filter('date')(data.createTime,'yyyy-MM-dd HH:mm:ss');
          data.finishTime = data.finished ? data.finishTime === 0 ? "-" :
            $filter('date')(data.finishTime,'yyyy-MM-dd HH:mm:ss') : '-';
          data.progress = Math.round(data.progress * 100);
          data.progressColor = data.finished ? data.successful ? 'success' : 'danger' : 'warning';
        });
        $scope.totalPage = Math.ceil($scope.totalNumber / $scope.pageNumber);
      }, function(errorResponse) {
        $scope.totalNumber = 0;
      });
  };
  $scope.gotoPage = function (index) {
    $scope.currentPage = index;
    getCopyActions();
  };
  $scope.defindOrderBy = function (filed) {
    if ($scope.orderby === filed) {
      $scope.isDesc = ! $scope.isDesc;
    } else {
      $scope.orderby = filed;
      $scope.isDesc = true;
    }
    getCopyActions();
  };

  getCopyActions();

  var timer=$interval(function(){
    getCopyActions();
  },5000);

  $scope.$on('$destroy',function(){
    $interval.cancel(timer);
  });
}

