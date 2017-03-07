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
(function() {
  "use strict";
    var i = 0;
    $('#btn-run-cmd').click(function () {

    var url = '/ssm/v1?op=RUNCOMMAND&cmd=' + $('#cmd').val()

    $.ajax({
        type: 'PUT',
        url: url
    }).then(function(data) {
        $('#stdout').html('');
        $('#stderr').html('');
        for (var i=0;i<data.stdout.length;i++) {
            $('#stdout').append(data.stdout[i]+'<br>');
        }

        for (var i=0;i<data.stderr.length;i++) {
            $('#stderr').append(data.stderr[i]+'<br>');
        }
    });

    });


//     $('[id^=btn-run-cmd]').click(function () {
//
//          var url = '/ssm/v1?op=RUNCOMMAND&cmd=' + $('[id^=btn-run-cmd]').parent().prev().val();
//          alert();
//
//          $.ajax({
//              type: 'PUT',
//              url: url
//          }).then(function(data) {
//              $('[id^=btn-run-cmd]').parent().parent().prev().children(":first").html('');
//              $('[id^=btn-run-cmd]').parent().parent().prev().children(":last").html('');
//              for (var i=0;i<data.stdout.length;i++) {
//                  $('[id^=btn-run-cmd]').parent().parent().prev().children(":first").append(data.stdout[i]+'<br>');
//              }
//
//              for (var i=0;i<data.stderr.length;i++) {
//                  $('[id^=btn-run-cmd]').parent().parent().prev().children(":last").append(data.stderr[i]+'<br>');
//              }
//          });

//     });


//    $('#btn-show-cache').click(function () {
//
//    var url = '/ssm/v1?op=SHOWCACHE'
//
//    $.ajax({
//        type: 'GET',
//        url: url
//    }).then(function(data) {
//       $('.cachestatus').text(data.cacheUsedPercentage);
//    });
//
//    });


     $('#btn-new').click(function () {
                 i = i+1;
                 var $div = $('<form onsubmit="return false;">'
                     +'<div class="input-group">'
                         +'<input type="text" class="form-control" id="cmd'+i+'"/>'
                         +'<span class="input-group-btn">'
                         +'<button class="btn btn-default" type="button" id="btn-run-cmd'+i+'">Run!</button>'
                         +'</span>'
                     +'</div>'
                     +'<div>'
                         +'<p id="stdout'+i+'">stdout is </p>'
                         +'<p id="stderr'+i+'">stderr is </p>'
                     +'</div>'
                 +'</form>');
                 $("#div").append($div);

     });



})();