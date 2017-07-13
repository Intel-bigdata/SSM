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
var rootPath = function () {
  var root = location.origin + location.pathname;
  return root.substring(0, root.lastIndexOf("/") + 1);
}();
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
      load: ['heliumService', function(heliumService) {
        return heliumService.load;
      }],
      cached0: ['models', function (models) {
        return models.$get.cachedfiles();
      }]
    };

    var hotTestFilesLoad = {
      load: ['heliumService', function(heliumService) {
        return heliumService.load;
      }],
      hotfiles0: ['models', function (models) {
        return models.$get.hotFiles();
      }]
    };

    $routeProvider
      .when('/', {
        templateUrl: 'app/home/home.html'
      })
      .when('/notebook/:noteId', {
        templateUrl: 'app/notebook/notebook.html',
        controller: 'NotebookCtrl',
        resolve: visBundleLoad
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
         load: ['heliumService', function(heliumService) {
            return heliumService.load;
          }],
          rules0: ['models', function (models) {
            return models.$get.rules();
          }]
        }
      })
      .when('/rules/rule/:ruleId', {
        templateUrl: 'app/dashboard/views/rules/rule/rule.html',
        controller: 'RuleCtrl',
        resolve: {
          load: ['heliumService', function(heliumService) {
            return heliumService.load;
          }],
          rule0: ['$route', 'models', function ($route, models) {
            return models.$get.rule($route.current.params.ruleId);
          }]
        }
      })
      .when('/actions', {
        templateUrl: 'app/dashboard/views/actions/actions.html',
        controller: 'ActionsCtrl',
        resolve: {
          load: ['heliumService', function(heliumService) {
            return heliumService.load;
          }],
          actions0: ['models', function (models) {
            return models.$get.actions();
          }],
          actionTypes: ['models', function (models) {
            return models.$get.actionTypes();
          }]
        }
      })
      .when('/actions/action/:actionId', {
        templateUrl: 'app/dashboard/views/actions/action/action.html',
        controller: 'ActionCtrl',
        resolve: {
          load: ['heliumService', function(heliumService) {
            return heliumService.load;
          }],
          action0: ['$route', 'models', function ($route, models) {
            return models.$get.action($route.current.params.actionId);
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
      .otherwise({
        redirectTo: '/'
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
    restapiRoot: 'http://localhost:8080/',
    restapiQueryInterval: 3 * 1000, // in milliseconds
    restapiQueryTimeout: 30 * 1000, // in milliseconds
    restapiTaskLevelMetricsQueryLimit: 100,
    loginUrl: 'http://localhost:8080/' + 'login'
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
