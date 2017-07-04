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
angular.module('dashboard')

    .config(['$stateProvider','$urlRouterProvider',
        function ($stateProvider) {
            'use strict';

            $stateProvider
                .state('cluster.hottestFiles',{
                url:'/fileInCache',
                templateUrl: 'views/cluster/cluster_hottestFiles.html',
                controller: 'HotFileCtrl',
                resolve: {
                    hotfiles0: ['models', function (models) {
                        return models.$get.hotFiles();
                    }]
                }
            })
            ;
        }]
    )
    /**
     * This controller is used to obtain app. All nested views will read status from here.
     */
    .controller('HotFileCtrl', ['$scope', '$state', '$sortableTableBuilder', 'i18n', 'helper', 'models', 'hotfiles0',
        function ($scope, $state, $stb, i18n, helper, models, hotfiles0) {
            'use strict';

            $scope.hotfilesTable = {
                cols: [
                    $stb.text('File ID').key('id').styleClass('col-md-2').done(),
                    $stb.text('File Path').key('filePath').styleClass('col-md-2').done(),
                    $stb.text('Access Count').key('accessCountNum').canSort().sortDefaultDescent().styleClass('col-md-2').done()
                ],
                rows: null
            };

            function updateHotTable(hotFiles) {
                $scope.hotfilesTable.rows = $stb.$update($scope.hotfilesTable.rows,
                    _.map(hotFiles, function (file) {
                        return {
                            id: file.fid,
                            filePath: file.path,
                            accessCountNum: file.accessCount
                        };
                    }));
            }

            updateHotTable(hotfiles0.$data());
            hotfiles0.$subscribe($scope, function (hotfiles) {
                updateHotTable(hotfiles);
            });
        }])
;