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
    /**
     * This controller is used to obtain app. All nested views will read status from here.
     */
    .controller('FileInCacheCtrl', FileInCacheCtrl);

    FileInCacheCtrl.$inject =
      ['$scope', '$sortableTableBuilder', 'cached0'];

        function FileInCacheCtrl ($scope, $stb, cached0) {
            $scope.filesTable = {
                cols: [
                    $stb.text('ID').key('id').canSort().sortDefaultDescent().styleClass('col-md-2').done(),
                    $stb.text('File Path').key('filePath').canSort().styleClass('col-md-2').done(),
                    $stb.datetime('Cached Time').key('cachedTime').canSort().styleClass('col-md-3').done(),
                    $stb.datetime('Last Accessed Time').key('lastTime').canSort().styleClass('col-md-3').done(),
                    $stb.text('Accessed Times').key('num').canSort().styleClass('col-md-2').done()
                ],
                rows: null
            };

            function updateTable(cachedFiles) {
                $scope.filesTable.rows = $stb.$update($scope.filesTable.rows,
                    _.map(cachedFiles.body, function (file) {
                        return {
                            id: file.fid,
                            filePath: file.path,
                            cachedTime: file.fromTime,
                            lastTime: file.lastAccessTime,
                            num: file.numAccessed
                        };
                    }));
            }

            updateTable(cached0.$data());
            cached0.$subscribe($scope, function (cachedfiles) {
                updateTable(cachedfiles);
            });
        }
