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

  .controller('NodesCtrl', NodesCtrl);
  NodesCtrl.$inject = ['$scope', '$filter', 'nodes0', 'serverHosts', 'agentHosts'];
  function NodesCtrl($scope, $filter, nodes0, serverHosts, agentHosts) {
    $scope.nodes = nodes0.body;
    // Smart Server hosts configured in conf/servers, obtained from SmartConf.
    $scope.serverHosts = serverHosts.body;
    // Smart Server hosts configured in conf/agents, obtained from SmartConf.
    $scope.agentHosts = agentHosts.body;

    angular.forEach($scope.nodes, function (data, index) {
      if ('maxInExecution' in data) {
        let index = $scope.serverHosts.indexOf(data.nodeInfo.host);
        if (index >= 0) {
          data.isLive = true;
          data.type = 'server';
          $scope.serverHosts.splice(index, 1);
        }
      } else {
        let index = $scope.agentHosts.indexOf(data.nodeInfo.host);
        if (index >= 0) {
          data.isLive = true;
          data.type = 'agent';
          $scope.agentHosts.splice(index, 1);
        } else {
          // The node host is not added in conf/servers,
          // but it may be launched after SSM cluster becomes active.
          data.isLive = true;
          data.type = 'agent';
        }
      }
    });

    $scope.liveNumber = $scope.nodes.length;
    $scope.deadNumber = $scope.serverHosts.length + $scope.agentHosts.length;

    angular.forEach($scope.serverHosts, function (host, index) {
      $scope.nodes.push(
        {
          nodeInfo: {
            host: host
          },
          isLive: false,
          type: 'server'
        }
      );
    });

    angular.forEach($scope.agentHosts, function (host, index) {
      $scope.nodes.push(
        {
          nodeInfo: {
            host: host
          },
          isLive: false,
          type: 'agent'
        }
      );
    });

    angular.forEach($scope.nodes, function (data,index) {
      data.registTime = data.registTime === 0 ? '-' :
        $filter('date')(data.registTime,'yyyy-MM-dd HH:mm:ss');
    });
  }
