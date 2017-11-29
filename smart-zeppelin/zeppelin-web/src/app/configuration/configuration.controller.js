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

angular.module('zeppelinWebApp').controller('ConfigurationCtrl', ConfigurationCtrl);

ConfigurationCtrl.$inject = ['$scope', '$rootScope', '$http', 'baseUrlSrv', 'ngToast', 'conf'];

function ConfigurationCtrl($scope, $rootScope, $http, baseUrlSrv, ngToast, conf) {
  $scope._ = _;
  ngToast.dismiss();
  $scope.conf = [];

  var getConf = function() {
    $http.get(baseUrlSrv.getRestApiRoot() + 'smart/api/' + conf.restapiProtocol + '/' + 'conf').
    success(function(data, status, headers, config) {
      $scope.conf = data.body;
    }).
    error(function(data, status, headers, config) {
      if (status === 401) {
        ngToast.danger({
          content: 'You don\'t have permission on this page',
          verticalPosition: 'bottom',
          timeout: '3000'
        });
        setTimeout(function() {
          window.location.replace('/');
        }, 3000);
      }
      console.log('Error %o %o', status, data.message);
    });
  };

  var initConf = function() {
    getConf();
  };

  initConf();
}
