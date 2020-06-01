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

angular.module('zeppelinWebApp').controller('NewPasswordCtrl', NewPasswordCtrl);

NewPasswordCtrl.$inject = ['$scope', '$rootScope', '$http',
  '$httpParamSerializer', 'baseUrlSrv', '$location', '$timeout'];
function NewPasswordCtrl($scope, $rootScope, $http, $httpParamSerializer, baseUrlSrv, $location, $timeout) {

  $scope.SigningIn = false;
  $scope.newPasswordCtrlParams = {};
  $scope.login = function() {

    $scope.SigningIn = true;
    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/login/newPassword',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      data: $httpParamSerializer({
        'userName': $scope.newPasswordCtrlParams.userName,
        'oldPassword': $scope.newPasswordCtrlParams.oldPassword,
        'newPassword1': $scope.newPasswordCtrlParams.newPassword1,
        'newPassword2': $scope.newPasswordCtrlParams.newPassword2
      })
    }).then(function successCallback(response) {
      $rootScope.ticket = response.data.body;
      angular.element('#loginModal').modal('toggle');
      $rootScope.$broadcast('loginSuccess', true);
      $rootScope.userName = $scope.newPasswordCtrlParams.userName;
      $scope.SigningIn = false;

      $location.path('/notebook');
    }, function errorCallback(errorResponse) {
      $scope.newPasswordCtrlParams.errorText =
      'The username and password that you entered don\'t match.';
      $scope.SigningIn = false;
    });

  };

  var initValues = function() {
    $scope.newPasswordCtrlParams = {
      userName: '',
      oldPassword: '',
      newPassword1: '',
      newPassword2: ''
    };
  };

  //handle session logout message received from WebSocket
  $rootScope.$on('session_logout', function(event, data) {
    if ($rootScope.userName !== '') {
      $rootScope.userName = '';
      $rootScope.ticket = undefined;

      setTimeout(function() {
        $scope.newPasswordCtrlParams = {};
        $scope.newPasswordCtrlParams.errorText = data.info;
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

