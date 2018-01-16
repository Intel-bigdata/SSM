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
angular.module('org.apache.hadoop.ssm.models', [])

  .factory('models', ['$timeout', 'conf', 'restapi', 'locator',
    function ($timeout, conf, restapi, locator) {
      'use strict';

      var util = {
        usage: function (current, total) {
          return total > 0 ? 100 * current / total : 0;
        },
        getOrCreate: function (obj, prop, init) {
          if (!obj.hasOwnProperty(prop)) {
            obj[prop] = init;
          }
          return obj[prop];
        },
        getProgress: function (base, running) {
          var result;
          if (running === 0 || base === 0) {
            result = 1;
          } else {
            result = Math.round((base - running) * 100 / base) /100;
          }
          return result;
        },
        parseIntFromQueryPathTail: function (path) {
          return Number(_.last(path.split('.')).replace(/[^0-9]/g, ''));
        },
        joinArguments: function (args) {
          var result = '';
          Object.keys(args).forEach(function(key) {
            result = result + ' ' + key + ' ' + args[key];
          });
          return result;
        }
      };

      /**
       * Retrieves a model from backend as a promise.
       * The resolved object will have two special methods.
       *   `$subscribe` - watch model changes within a scope.
       *   `$data` - return pure model data without these two methods.
       */
      function get(path, decodeFn, args) {
        args = args || {};
        return restapi.get(path).then(function (response) {
          var oldModel;
          var model = decodeFn(response.data, args);

          model.$subscribe = function (scope, onData, onError) {
            restapi.subscribe(args.pathOverride || path, scope, function (data) {
              try {
                var newModel = decodeFn(data, args);
                if (!_.isEqual(newModel, oldModel)) {
                  oldModel = newModel;
                  return onData(newModel);
                }
              } catch (ex) {
                if (angular.isFunction(onError)) {
                  return onError(data);
                }
              }
            }, args.period);
          };

          model.$data = function () {
            return _.omit(model, _.isFunction);
          };

          return model;
        });
      }

      var decoder = {
        _asAssociativeArray: function (objs, decodeFn, keyName) {
          var result = {};
          _.map(objs.body, function (obj) {
            var model = decodeFn(obj);
            var key = model[keyName];
            result[key] = model;
          });
          return result;
        },
        default: function (obj) {
          return obj;
        },
        rules: function (objs) {
          return decoder._asAssociativeArray(objs, decoder.ruleSummary, 'id');
        },
        movers: function (objs) {
          return decoder._asAssociativeArray(objs, decoder.moverSummary, 'id');
        },
        copys: function (objs) {
          return decoder._asAssociativeArray(objs, decoder.copySummary, 'id');
        },
        ruleSummary: function (obj) {
          return angular.merge(obj, {
            // extra properties
            ruleName: 'Rule ' + obj.id,
            isRunning: (obj.state === 'ACTIVE' || obj.state === 'DRYRUN'),
            isDead: !(obj.state === 'ACTIVE' || obj.state === 'DRYRUN'),
            isDelete: (obj.state === 'DELETED'),
            // extra methods
            pageUrl: locator.rule(obj.id),
            start: function () {
              return restapi.startRule(obj.id);
            },
            terminate: function () {
              return restapi.stopRule(obj.id);
            },
            delete: function () {
              return restapi.deleteRule(obj.id);
            }
          });
        },
        moverSummary: function (obj) {
          return angular.merge(obj, {
            isRunning: (obj.state === 'ACTIVE' || obj.state === 'DRYRUN'),
            pageUrl: locator.mover(obj.id),
            progress: util.getProgress(obj.baseProgress, obj.runningProgress),
            start: function () {
              return restapi.startRule(obj.id);
            },
            terminate: function () {
              return restapi.stopRule(obj.id);
            },
            delete: function () {
              return restapi.deleteRule(obj.id);
            }
          });
        },
        copySummary: function (obj) {
          return angular.merge(obj, {
            isRunning: (obj.state === 'ACTIVE' || obj.state === 'DRYRUN'),
            pageUrl: locator.copy(obj.id),
            progress: util.getProgress(obj.baseProgress, obj.runningProgress),
            start: function () {
              return restapi.startRule(obj.id);
            },
            terminate: function () {
              return restapi.stopRule(obj.id);
            },
            delete: function () {
              return restapi.deleteRule(obj.id);
            }
          });
        },
        rule: function (obj) {
          obj = obj.body;
          angular.merge(obj, {
            ruleName: 'Rule ' + obj.id,
          });
          return obj;
        },
        /** Return an array of application alerts */
        ruleAlerts: function (obj) {
          if (obj.time > 0) {
            return [{
              severity: 'error',
              time: Number(obj.time),
              message: obj.error
            }];
          }
          return [];
        },
        ruleCmdlets: function (objs) {
          return decoder._asAssociativeArray(objs, decoder.cmdlet, 'cid');
        },
        cmdlet: function (obj) {
          return angular.merge(obj, {
            // extra properties
            isRunning: obj.state === 'EXECUTING',
             // extra methods
            pageUrl: locator.cmdlet(obj.rid, obj.cid)
          });
        },
        actions: function (objs) {
          return decoder._asAssociativeArray(objs, decoder.action, 'actionId');
        },
        nodes: function (objs) {
          return objs;
        },
        action: function (obj) {
          return angular.merge(obj, {
            uptime: obj.finishTime - obj.createTime,
            status: obj.finished ? 'Finished' : 'Running',
            pageUrl: locator.action(obj.actionId),
            argument: util.joinArguments(obj.args)
          });
        },
        actionInfo: function (obj) {
          obj = obj.body;
          return angular.merge(obj, {
            uptime: obj.finishTime - obj.createTime,
            status: obj.finished ? 'Finished' : 'Running',
            pageUrl: locator.action(obj.actionId),
            argument: util.joinArguments(obj.args)
          });
        }
      };

      var getter = {
        rules: function () {
          return get('rules/list', decoder.rules);
        },
        rule: function (ruleId) {
          return get('rules/' + ruleId + '/info', decoder.rule);
        },
        ruleAlerts: function (ruleId) {
          return get('rules/' + ruleId + '/errors', decoder.ruleAlerts);
        },
        ruleCmdlets: function (ruleId) {
          return get('rules/' + ruleId + '/cmdlets', decoder.ruleCmdlets);
        },
        movers: function () {
          return get('rules/list/move', decoder.movers);
        },
        copys: function () {
          return get('rules/list/sync', decoder.copys);
        },
        moverActions: function (ruleId) {
          return get('actions/filelist/0/' + ruleId , decoder.actions);
        },
        copyActions: function (ruleId) {
          return get('actions/filelist/0/' + ruleId , decoder.actions);
        },
        actions: function () {
          return get('actions/list/0', decoder.actions);
        },
        actionInfo: function (actionId) {
          return get('actions/' + actionId + '/info', decoder.actionInfo);
        },
        actionTypes: function () {
          return get('actions/registry/list', decoder.default)
        },
        cachedfiles: function () {
          return get('cluster/primary/cachedfiles', decoder.default)
        },
        hotFiles: function () {
          return get('cluster/primary/hotfiles', decoder.default)
        },
        storageUsage: function (resourceName) {
          return get('cluster/primary/utilization/' + resourceName, decoder.default)
        },
        nodes: function () {
          return get('cluster/primary/ssmnodesinfo', decoder.nodes);
        },
        notebookInfo: function () {
          return get('note/info', decoder.default)
        }
      };

      return {
        $get: getter,
        /** Attempts to get model and then subscribe changes as long as the scope is valid. */
        $subscribe: function (scope, getModelFn, onData, period) {
          var shouldCancel = false;
          var promise;
          scope.$on('$destroy', function () {
            shouldCancel = true;
            $timeout.cancel(promise);
          });
          function trySubscribe() {
            if (shouldCancel) {
              return;
            }
            getModelFn().then(function (data) {
              return onData(data);
            }, /*onerror=*/function () {
              promise = $timeout(trySubscribe, period || conf.restapiQueryInterval);
            });
          }

          trySubscribe();
        },
        DAG_DEATH_UNSPECIFIED: '9223372036854775807' /* Long.max */
      };
    }])
;
