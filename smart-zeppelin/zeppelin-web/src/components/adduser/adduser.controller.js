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

angular.module('zeppelinWebApp').controller('AddUserCtrl', AddUserCtrl);

AddUserCtrl.$inject = ['$scope', '$rootScope', '$http',
  '$httpParamSerializer', 'baseUrlSrv', '$location', '$timeout'];
function AddUserCtrl($scope, $rootScope, $http, $httpParamSerializer, baseUrlSrv, $location, $timeout) {

  $scope.registering = false;
  $scope.addUserCtrlParams = {};
  $scope.addUserFunction = function() {
    $scope.registering = true;
    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/login/adduser',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      data: $httpParamSerializer({
        'adminPassword': $scope.addUserCtrlParams.adminPassword,
        'userName': $scope.addUserCtrlParams.userName,
        'password1': $scope.addUserCtrlParams.password1,
        'password2': $scope.addUserCtrlParams.password2
      })
    }).then(function successCallback(response) {
      $scope.addUserCtrlParams.responseText = 'Successfully registered!';
      $scope.registering = false;
    }, function errorCallback(errorResponse) {
      $scope.addUserCtrlParams.responseText = errorResponse.data.message;
      $scope.registering = false;
    });

  };

  var initValues = function() {
    $scope.addUserCtrlParams = {
      adminPassword: '',
      userName: '',
      password1: '',
      password2: ''
    };
  };

  //handle session logout message received from WebSocket
  $rootScope.$on('session_logout', function(event, data) {
    if ($rootScope.userName !== '') {
      $rootScope.userName = '';
      $rootScope.ticket = undefined;

      setTimeout(function() {
        $scope.addUserCtrlParams = {};
        $scope.addUserCtrlParams.responseText = data.info;
        angular.element('.nav-login-btn').click();
      }, 1000);
      var locationPath = $location.path();
      $location.path('/').search('ref', locationPath);
    }
  });

  /*
   ** $scope.$on functions below
   */
  $scope.$on('initLoginValues', function() {
    initValues();
  });
}

