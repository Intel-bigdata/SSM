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

/** TODO: refactoring work required */
  .factory('restapi', ['$q', '$http', '$timeout', '$modal', 'Upload', 'conf', 'HealthCheckService',
    function ($q, $http, $timeout, $modal, Upload, conf, HealthCheckService) {
      'use strict';

      function decodeSuccessResponse(data) {
        return angular.merge({
          success: true
        }, (data || {}));
      }

      function decodeErrorResponse(data) {
        var errorMessage = '';
        var stackTrace = [];
        var lines = (data || '').split('\n');
        if (lines.length) {
          errorMessage = lines[0].replace(', error summary:', '');
          stackTrace = lines.slice(1);
        }
        return {success: false, error: errorMessage, stackTrace: stackTrace};
      }

      var restapiV1Root = conf.restapiRoot + 'smart/api/' + conf.restapiProtocol + '/';
      var self = {
        /**
         * Retrieve data from rest service endpoint (HTTP GET) periodically in an angular scope.
         */
        subscribe: function (path, scope, onData, interval) {
          var timeoutPromise;
          var shouldCancel = false;
          scope.$on('$destroy', function () {
            shouldCancel = true;
            $timeout.cancel(timeoutPromise);
          });

          interval = interval || conf.restapiQueryInterval;
          var fn = function () {
            var promise = self.get(path);
            promise.then(function (response) {
              if (!shouldCancel && angular.isFunction(onData)) {
                shouldCancel = onData(response.data);
              }
            }, function (response) {
              if (!shouldCancel && angular.isFunction(onData)) {
                shouldCancel = onData(response.data);
              }
            })
              .finally(function () {
                if (!shouldCancel) {
                  timeoutPromise = $timeout(fn, interval);
                }
              });
          };
          timeoutPromise = $timeout(fn, interval);
        },

        /**
         * Query model from service endpoint and return a promise.
         * Note that if operation is failed, it will return the failure after a default timeout. If
         * health check indicates the service is unavailable, no request will be sent to server, just
         * simple return a failure after a default timeout.
         */
        get: function (path) {
          if (!HealthCheckService.isServiceAvailable()) {
            var deferred = $q.defer();
            _.delay(deferred.reject, conf.restapiQueryTimeout);
            return deferred.promise;
          }
          return $http.get(restapiV1Root + path, {timeout: conf.restapiQueryTimeout});
        },

        /** Get data from server periodically until an user cancellation or scope exit. */
        repeatUntil: function (url, scope, onData) {
          // TODO: Once `subscribe` is turned to websocket push model, there is no need to have this method
          this.subscribe(url, scope,
            function (data) {
              return !onData || onData(data);
            });
        },

        /** Kill a running application */
        startRule: function (ruleId) {
          var url = restapiV1Root + 'rules/' + ruleId + "/start";
          return $http.post(url);
        },

        /** Kill a running application */
        stopRule: function (ruleId) {
          var url = restapiV1Root + 'rules/' + ruleId + "/stop";
          return $http.post(url);
        },

        /** Submit an user defined application with user configuration */
        submitRule: function (args, onComplete) {
          return self._submitRule(restapiV1Root + 'rules/add', args, onComplete);
        },

        _submitRule: function (url, args, onComplete) {
          return $http({
            method: 'POST',
            url: url,
            data: $.param({ruleText: args}),
            headers: {'Content-Type': 'application/x-www-form-urlencoded'}
          }).then(function (response) {
            if (onComplete) {
              onComplete(decodeSuccessResponse(response.data));
            }
          }, function (response) {
            if (onComplete) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Submit an user defined application with user configuration */
        submitAction: function (action, args, onComplete) {
          return self._submitAction(restapiV1Root + 'cmdlets/submit', action, args, onComplete);
        },

        _submitAction: function (url, action, args, onComplete) {
          return $http({
            method: 'POST',
            url: url,
            data: action + ' ' + args,
            headers: {'Content-Type': 'application/raw'}
          }).then(function (response) {
            if (onComplete) {
              onComplete(decodeSuccessResponse(response.data));
            }
          }, function (response) {
            if (onComplete) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Upload a set of JAR files */
        uploadJars: function (files, onComplete) {
          var upload = Upload.upload({
            url: restapiV1Root + 'master/uploadjar',
            method: 'POST',
            file: files,
            fileFormDataName: 'jar'
          });

          upload.then(function (response) {
            if (onComplete) {
              onComplete(decodeSuccessResponse({files: response.data}));
            }
          }, function (response) {
            if (onComplete) {
              onComplete(decodeErrorResponse(response.data));
            }
          });
        },

        /** Return the service version in onData callback */
        serviceVersion: function (onData) {
          return $http.get(conf.restapiRoot + 'version').then(function (response) {
            if (angular.isFunction(onData)) {
              onData(response.data);
            }
          });
        }
      };
      return self;
    }
  ])
;
