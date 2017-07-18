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
 * You can override any values below in your application
 */
  .constant('i18n', {
    terminology: {
      master: 'Every cluster has one or more Master node. Master node is responsible to manage global resources of the cluster.',
      worker: 'Worker node is responsible to manage local resources on single machine',
      workerExecutor: 'Executor JVMs running on current worker node.',
      appClock: 'Application clock tracks minimum clock of all message timestamps. Message timestamp is immutable since birth. It denotes the moment when message is generated.',
      cmdlet: 'Action is the minimum unit of execution. A cmdlet can contain more than one actions. Different cmdlets can be executed at the same time, but actions belonging to a cmdlet can only be executed in sequence. The cmdlet get executed when rule conditions fulfills.',
      processor: 'For streaming application type, each application contains a topology, which is a DAG (directed acyclic graph) to describe the data flow. Each node in the DAG is a processor.',
      task: 'For streaming application type, Task is the minimum unit of parallelism. In runtime, each Processor is paralleled to a list of tasks, with different tasks running in different executor.'
    }
  })
;
