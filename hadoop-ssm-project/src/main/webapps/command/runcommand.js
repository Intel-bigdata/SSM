/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"use strict";
function run(var1) {
    var number = var1.substring(var1.length-1);
    alert(number);
    var url = '/ssm/v1?op=RUNCOMMAND&cmd=' + $('#cmd'+number).val();
    $.ajax({
        type: 'PUT',
        url: url
    }).then(function(data) {
        $('#stdout'+number).html('');
        $('#stderr'+number).html('');
        for (var j=0;j<data.stdout.length;j++) {
            $('#stdout'+number).append(data.stdout[j]+'<br>');
        }
        for (var j=0;j<data.stderr.length;j++) {
            $('#stderr'+number).append(data.stderr[j]+'<br>');
        }
    });
};

var i = 0;
function newDiv() {
    i = i+1;
    var $div = $('<form onsubmit="return false;">'
     +'<div class="input-group">'
         +'<input type="text" class="form-control" id="cmd'+i+'"/>'
         +'<span class="input-group-btn">'
         +'<button class="btn btn-default" type="button" onclick="run(this.id)" id="btn-run-cmd'+i+'">Run!</button>'
         +'</span>'
     +'</div>'
     +'<div>'
         +'<p id="stdout'+i+'">stdout is </p>'
         +'<p id="stderr'+i+'">stderr is </p>'
     +'</div>'
    +'</form>');
    $("#div").append($div);

};

