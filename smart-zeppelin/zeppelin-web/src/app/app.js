/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// rootPath of this site, it has a tailing slash /
var zeppelinWebApp = angular.module('zeppelinWebApp', [
  'ngCookies',
  'ngAnimate',
  'ngRoute',
  'ngSanitize',
  'angular-websocket',
  'ui.ace',
  'ui.bootstrap',
  'as.sortable',
  'ngTouch',
  'ngDragDrop',
  'angular.filter',
  'monospaced.elastic',
  'puElasticInput',
  'xeditable',
  'ngToast',
  'focus-if',
  'ngResource',
  'mgcrea.ngStrap',
  'ui.select',
  'cfp.loadingBarInterceptor',
  'ngFileUpload',
  'dashing',
  'org.apache.hadoop.ssm.models',
  'ngclipboard'
])
  .filter('breakFilter', function() {
    return function(text) {
      if (!!text) {
        return text.replace(/\n/g, '<br />');
      }
    };
  })
  .config(function($httpProvider, $routeProvider, ngToastProvider) {
    // withCredentials when running locally via grunt
    $httpProvider.defaults.withCredentials = true;

    var visBundleLoad = {
      load: ['heliumService', function(heliumService) {
        return heliumService.load;
      }]
    };

    var fileInCacheLoad = {
      cached0: ['models', function (models) {
        return models.$get.cachedfiles();
      }]
    };

    var hotTestFilesLoad = {
      hotfiles0: ['models', function (models) {
        return models.$get.hotFiles();
      }]
    };

    $routeProvider
      .when('/', {
        templateUrl: 'components/login/login.html',
        controller: 'LoginCtrl'
      })
      .when('/notebook', {
        templateUrl: 'app/notebook/notebook.html',
        controller: 'NotebookCtrl',
        resolve: {
          rules0: ['models', function (models) {
            return models.$get.rules();
          }]
        }
      })
      .when('/notebook/:noteId/paragraph?=:paragraphId', {
        templateUrl: 'app/notebook/notebook.html',
        controller: 'NotebookCtrl',
        resolve: visBundleLoad
      })
      .when('/notebook/:noteId/paragraph/:paragraphId?', {
        templateUrl: 'app/notebook/notebook.html',
        controller: 'NotebookCtrl',
        resolve: visBundleLoad
      })
      .when('/notebook/:noteId/revision/:revisionId', {
        templateUrl: 'app/notebook/notebook.html',
        controller: 'NotebookCtrl',
        resolve: visBundleLoad
      })
      .when('/cluster/hotTestFiles', {
        templateUrl: 'app/dashboard/views/cluster/cluster_hottestFiles.html',
        controller: 'HotFileCtrl',
        resolve: hotTestFilesLoad
      })
      .when('/cluster/fileInCache', {
        templateUrl: 'app/dashboard/views/cluster/cluster_fileInCache.html',
        controller: 'FileInCacheCtrl',
        resolve: fileInCacheLoad
      })
      .when('/rules', {
        templateUrl: 'app/dashboard/views/rules/rules.html',
        controller: 'RulesCtrl',
        resolve: {
          rules0: ['models', function (models) {
            return models.$get.rules();
          }]
        }
      })
      .when('/rule/grammar', {
        templateUrl: 'app/dashboard/views/rules/submit/help.html'
      })
      .when('/rules/rule/:ruleId', {
        templateUrl: 'app/dashboard/views/rules/rule/rule.html',
        controller: 'RuleCtrl',
        resolve: {
          rule0: ['$route', 'models', function ($route, models) {
            return models.$get.rule($route.current.params.ruleId);
          }]
        }
      })
      .when('/movers', {
        templateUrl: 'app/dashboard/views/mover/mover.html',
        controller: 'MoverCtrl',
        resolve: {
          movers0: ['models', function (models) {
            return models.$get.movers();
          }]
        }
      })
      .when('/movers/mover/:ruleId', {
        templateUrl: 'app/dashboard/views/mover/detail/moverActions.html',
        controller: 'MoverActionsCtrl'
      })
      .when('/actions', {
        templateUrl: 'app/dashboard/views/actions/actions.html',
        controller: 'ActionsCtrl'
      })
      .when('/action/usage', {
        templateUrl: 'app/dashboard/views/actions/submit/help.html'
      })
      .when('/syncs', {
        templateUrl: 'app/dashboard/views/copy/copy.html',
        controller: 'CopyCtrl',
        resolve: {
          copys0: ['models', function (models) {
            return models.$get.copys();
          }]
        }
      })
      .when('/copys/copy/:ruleId', {
        templateUrl: 'app/dashboard/views/copy/detail/copyActions.html',
        controller: 'CopyActionsCtrl'
      })
      .when('/actions/action/:actionId', {
        templateUrl: 'app/dashboard/views/actions/action/action.html',
        controller: 'ActionCtrl',
        resolve: {
          action0: ['$route', 'models', function ($route, models) {
            return models.$get.actionInfo($route.current.params.actionId);
          }]
        }
      })
      .when('/node/info', {
        templateUrl: 'app/dashboard/views/cluster/nodeinfo/nodes.html',
        controller: 'NodesCtrl',
        resolve: {
          nodes0: ['models', function (models) {
            return models.$get.nodes();
          }]
        }
      })
      .when('/jobmanager', {
        templateUrl: 'app/jobmanager/jobmanager.html',
        controller: 'JobmanagerCtrl'
      })
      .when('/interpreter', {
        templateUrl: 'app/interpreter/interpreter.html',
        controller: 'InterpreterCtrl'
      })
      .when('/notebookRepos', {
        templateUrl: 'app/notebookRepos/notebookRepos.html',
        controller: 'NotebookReposCtrl',
        controllerAs: 'noterepo'
      })
      .when('/credential', {
        templateUrl: 'app/credential/credential.html',
        controller: 'CredentialCtrl'
      })
      .when('/helium', {
        templateUrl: 'app/helium/helium.html',
        controller: 'HeliumCtrl'
      })
      .when('/configuration', {
        templateUrl: 'app/configuration/configuration.html',
        controller: 'ConfigurationCtrl'
      })
      .when('/search/:searchTerm', {
        templateUrl: 'app/search/result-list.html',
        controller: 'SearchResultCtrl'
      })
      .when('/storage', {
        templateUrl: 'app/dashboard/views/cluster/storage/storages.html',
        controller: 'StoragesCtrl'
      })
      .otherwise({
        redirectTo: '/notebook'
      });

    ngToastProvider.configure({
      dismissButton: true,
      dismissOnClick: false,
      combineDuplications: true,
      timeout: 6000
    });
  })

  //handel logout on API failure
  .config(function ($httpProvider, $provide) {
    $provide.factory('httpInterceptor', function ($q, $rootScope) {
      return {
        'responseError': function (rejection) {
          if (rejection.status === 405) {
            var data = {};
            data.info = '';
            $rootScope.$broadcast('session_logout', data);
          }
          $rootScope.$broadcast('httpResponseError', rejection);
          return $q.reject(rejection);
        }
      };
    });
    $httpProvider.interceptors.push('httpInterceptor');
  })
  .config(['cfpLoadingBarProvider', function (cfpLoadingBarProvider) {
    cfpLoadingBarProvider.includeSpinner = false;
    cfpLoadingBarProvider.latencyThreshold = 1000;
  }])

  // configure angular-strap
  .config(['$tooltipProvider', function ($tooltipProvider) {
    angular.extend($tooltipProvider.defaults, {
      html: true
    });
  }])

  // configure dashing
  .config(['dashing.i18n', function (i18n) {
    i18n.confirmationYesButtonText = 'OK';
    i18n.confirmationNoButtonText = 'Cancel';
  }])

  // disable logging for production
  .config(['$compileProvider', function ($compileProvider) {
    $compileProvider.debugInfoEnabled(false);
  }])

  // constants
  .constant('conf', {
    restapiProtocol: 'v1',
    restapiQueryInterval: 3 * 1000, // in milliseconds
    restapiQueryTimeout: 30 * 1000, // in milliseconds
    restapiTaskLevelMetricsQueryLimit: 100
  })
  .constant('TRASH_FOLDER_ID', '~Trash');

function auth() {
  var $http = angular.injector(['ng']).get('$http');
  var baseUrlSrv = angular.injector(['zeppelinWebApp']).get('baseUrlSrv');
  // withCredentials when running locally via grunt
  $http.defaults.withCredentials = true;
  jQuery.ajaxSetup({
    dataType: 'json',
    xhrFields: {
      withCredentials: true
    },
    crossDomain: true
  });
  return $http.get(baseUrlSrv.getRestApiBase() + '/security/ticket').then(function(response) {
    zeppelinWebApp.run(function($rootScope) {
      $rootScope.ticket = angular.fromJson(response.data).body;
    });
  }, function(errorResponse) {
    // Handle error case
  });
}

function bootstrapApplication() {
  zeppelinWebApp.run(function($rootScope, $location) {
    $rootScope.$on('$routeChangeStart', function(event, next, current) {
      if (!$rootScope.ticket && next.$$route && !next.$$route.publicAccess) {
        $location.path('/');
      }
    });
  });
  angular.bootstrap(document, ['zeppelinWebApp']);
}

angular.element(document).ready(function () {
  auth().then(bootstrapApplication)
})

// See ZEPPELIN-2577. No graphs visible on IE 11
// Polyfill. https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/String/endsWith
if (!String.prototype.endsWith) {
  // eslint-disable-next-line no-extend-native
  String.prototype.endsWith = function(searchString, position) {
    let subjectString = this.toString()
    if (typeof position !== 'number' || !isFinite(position) ||
        Math.floor(position) !== position || position > subjectString.length) {
      position = subjectString.length
    }
    position -= searchString.length
    let lastIndex = subjectString.indexOf(searchString, position)
    return lastIndex !== -1 && lastIndex === position
  }
}
