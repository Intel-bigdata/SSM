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
  ActionsCtrl.$inject = ['$scope', 'baseUrlSrv', '$filter', '$http', 'conf', '$interval'];
  function ActionsCtrl($scope, baseUrlSrv, $filter, $http, conf, $interval) {
    $scope.pageNumber = 20;
    $scope.totalNumber = 0;
    $scope.actions;
    $scope.currentPage = 1;
    $scope.totalPage = 1;
    $scope.orderby = 'aid';
    $scope.isDesc = true;
    $scope.searching = false;
    $scope.currentSearchPage = 1;
    $scope.path;

    function getActions() {
      var url = baseUrlSrv.getSmartApiRoot() + conf.restapiProtocol + '/actions/list/'
        + $scope.currentPage + '/' + $scope.pageNumber + '/' + $scope.orderby + '/' + $scope.isDesc;
      setCookie(url, "");
      setVariables(url);
    };

    function __search__ (text) {
      if (!$scope.searching) {
        $scope.currentSearchPage = 1;
      }
      var url = baseUrlSrv.getSmartApiRoot() + conf.restapiProtocol + '/actions/search/'
        + text + '/' + $scope.currentSearchPage + '/' + $scope.pageNumber + '/' + $scope.orderby + '/' + $scope.isDesc;
      setCookie(url, decodeURIComponent(text));
      setVariables(url);
    }

    function setVariables(url) {
      $http.get(url)
        .then(function(response) {
        var actionData = angular.fromJson(response.data);
        $scope.totalNumber = actionData.body.totalNumOfActions;
        $scope.actions = actionData.body.actions;
        angular.forEach($scope.actions, function (data,index) {
          data.runTime = data.finishTime - data.createTime;
          data.createTime = data.createTime === 0 ? "-" :
            $filter('date')(data.createTime,'yyyy-MM-dd HH:mm:ss');
          data.finishTime = data.finished ? data.finishTime === 0 ? "-" :
            $filter('date')(data.finishTime,'yyyy-MM-dd HH:mm:ss') : '-';
          data.progress = Math.round(data.progress * 100);
          data.progressColor = data.finished ? data.successful ? 'success' : 'danger' : 'warning';
          for(var key in data.args) {
            var value = data.args[key];
            data.actionName += (' ' + key + ' ' + value);
          }
        });
        $scope.totalPage = Math.ceil($scope.totalNumber / $scope.pageNumber);
      }, function(errorResponse) {
          $scope.totalNumber = 0;
      });
    }

    function search(text) {
      if (text == "") {
        $scope.searching = false;
        getActions();
      }
      else {
        __search__(text);
        $scope.searching = true;
      }
    };

    function setCookie(curl, cvalue) {
        document.cookie = "tmpURL" + "=" + curl + ";" + ";path=/";
        document.cookie = "tmpSearch" + "=" + cvalue + ";" + ";path=/";
    }

    function getCookie(cname) {
        var name = cname + "=";
        var decodedCookie = decodeURIComponent(document.cookie);
        var ca = decodedCookie.split(';');
        for(var i = 0; i < ca.length; i++) {
            var c = ca[i];
            while (c.charAt(0) == ' ') {
                c = c.substring(1);
            }
            if (c.indexOf(name) == 0) {
                return c.substring(name.length, c.length);
            }
        }
        return "";
    }

    function checkCookie() {
        var url=getCookie("tmpURL");
        var search=getCookie("tmpSearch");
        if (url != "") {
          document.getElementById('search').value = search;
          setVariables(url);
        } else {
          getActions();
        }
    }

    $scope.getContent = function () {
      var tmp = document.getElementById('search').value;
      $scope.path = encodeURIComponent(tmp);
      search($scope.path);
    };

    $scope.jumpToPage = function () {
      var index = document.getElementById('page').value;
      $scope.gotoPage(Number(index));
    };

    $scope.gotoPage = function (index) {
      if (!$scope.searching) {
        $scope.currentPage = index;
        getActions();
      }
      else {
        $scope.currentSearchPage = index;
        __search__($scope.path);
      }
    };

    $scope.defindOrderBy = function (filed) {
      if ($scope.orderby === filed) {
        $scope.isDesc = ! $scope.isDesc;
      } else {
        $scope.orderby = filed;
        $scope.isDesc = true;
      }
      if (!$scope.searching) {
        getActions();
      }
      else {
        __search__($scope.path);
      }
    };

    // getActions();
    if ($scope.totalNumber == 0) {
      // $scope.currentPage = 0;
      // getActions();
      // $scope.currentPage = 1;
      checkCookie();
    }

    var input = document.getElementById("search");
    input.addEventListener("keyup", function(event) {
        event.preventDefault();
        if (event.keyCode === 13) {
            document.getElementById("searchBtn").click();
        }
    });

    $(document).keyup(function(event) {
        if (event.keyCode == 27) {
          $scope.searching = false;
          getActions();
          document.getElementById("search").value = "";
        }
    });

    var timer = $interval(function(){
      if (!$scope.searching) {
        getActions();
      }
      else {
        __search__($scope.path);
      }
    },2000);

    $scope.$on('$destroy',function(){
      $interval.cancel(timer);
    });
  }
