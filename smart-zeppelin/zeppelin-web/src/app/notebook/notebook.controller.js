/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

angular.module('zeppelinWebApp').controller('NotebookCtrl', NotebookCtrl);

NotebookCtrl.$inject = [
  '$scope',
  'ngToast',
  'rules0',
  '$rootScope'
];

function NotebookCtrl($scope, ngToast, rules0, $rootScope) {

  ngToast.dismiss();

  $scope.note = null;
  $scope.rules = rules0.$data();
  rules0.$subscribe($scope, function (rules) {
    $scope.rules = rules;
  });

  $scope.getCronOptionNameFromValue = function(value) {
    if (!value) {
      return '';
    }

    for (var o in $scope.cronOption) {
      if ($scope.cronOption[o].value === value) {
        return $scope.cronOption[o].name;
      }
    }
    return value;
  };

  /** Init the new controller */
  var initNotebook = function() {
    $scope.note = $rootScope.note;
  };

  initNotebook();

  // register mouseevent handler for focus paragraph
  document.addEventListener('click', $scope.focusParagraphOnClick);

  $scope.keyboardShortcut = function(keyEvent) {
    // handle keyevent
    if (!$scope.viewOnly && !$scope.revisionView) {
      $scope.$broadcast('keyEvent', keyEvent);
    }
  };

  // register mouseevent handler for focus paragraph
  document.addEventListener('keydown', $scope.keyboardShortcut);

  $scope.paragraphOnDoubleClick = function(paragraphId) {
    $scope.$broadcast('doubleClickParagraph', paragraphId);
  };

  $scope.isNoteRunning = function() {
    if (!$scope.note) { return false; }
    for (var i = 0; i < $scope.note.paragraphs.length; i++) {
      const status = $scope.note.paragraphs[i].status;
      if (status === 'PENDING' || status === 'RUNNING') {
        return true;
      }
    }
    return false;
  };

  $scope.cloneRule = function (rule) {
    if ($scope.note.paragraphs[0].text) {
      if (confirm("Are you sure to overwrite existing rule?")) {
        $scope.note.paragraphs[0].text = rule;
      };
    } else {
      $scope.note.paragraphs[0].text = rule;
    }
  };

  angular.element(document).click(function() {
    angular.element('.ace_autocomplete').hide();
  });

  $scope.$on('$destroy', function() {
    angular.element(window).off('beforeunload');
    // $scope.killSaveTimer();
    /*$scope.saveNote();*/

    document.removeEventListener('click', $scope.focusParagraphOnClick);
    document.removeEventListener('keydown', $scope.keyboardShortcut);
  });

  $scope.$on('$unBindKeyEvent', function() {
    document.removeEventListener('click', $scope.focusParagraphOnClick);
    document.removeEventListener('keydown', $scope.keyboardShortcut);
  });
}
