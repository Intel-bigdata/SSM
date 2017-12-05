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

angular.module('zeppelinWebApp').controller('ParagraphCtrl', ParagraphCtrl);

ParagraphCtrl.$inject = [
  '$scope',
  '$rootScope',
  '$route',
  '$window',
  '$routeParams',
  '$location',
  '$timeout',
  '$q',
  'noteVarShareService',
  'restapi'
];

function ParagraphCtrl($scope, $rootScope, $route, $window, $routeParams, $location,
                       $timeout, $q, noteVarShareService, restapi) {
  $rootScope.keys = Object.keys;
  $scope.parentNote = null;
  $scope.paragraph = null;
  $scope.originalText = '';
  $scope.editor = null;

  var editorSetting = {};
  // flag that is used to set editor setting on paste percent sign
  var pastePercentSign = false;
  // flag that is used to set editor setting on save interpreter bindings
  var setInterpreterBindings = false;
  var paragraphScope = $rootScope.$new(true, $rootScope);

  // Controller init
  $scope.init = function(newParagraph, note) {
    $scope.paragraph = newParagraph;
    $scope.parentNote = note;
    $scope.originalText = angular.copy(newParagraph.text);
    $scope.chart = {};
    $scope.baseMapOption = ['Streets', 'Satellite', 'Hybrid', 'Topo', 'Gray', 'Oceans', 'Terrain'];
    $scope.colWidthOption = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    $scope.paragraphFocused = false;
    if (newParagraph.focus) {
      $scope.paragraphFocused = true;
    }
    if (!$scope.paragraph.config) {
      $scope.paragraph.config = {};
    }

    noteVarShareService.put($scope.paragraph.id + '_paragraphScope', paragraphScope);

    initializeDefault($scope.paragraph.config);
  };

  var submit = function (response) {
    var results = {};
    var msg = new Array();
    $scope.uploading = false;
    var result = {};
    result.type = "TEXT";
    if (response.success) {
      results.code = "SUCCESS";
      $scope.paragraph.status = "FINISHED";
      if ($scope.paragraph.id === 'add_rule') {
        result.data = "Success to add rule : " + response.body;
      } else if ($scope.paragraph.id === 'run_action') {
        result.data = "Success to submit action : " + response.body;
      }
    } else {
      results.code = "ERROR";
      $scope.paragraph.status = "ERROR";
      result.data = response.error;
    }
    msg.push(result);
    results.msg = msg;
    $scope.paragraph.results = results;
  };

  var initializeDefault = function(config) {
    var forms = $scope.paragraph.settings.forms;

    if (!config.colWidth) {
      config.colWidth = 12;
    }

    if (config.enabled === undefined) {
      config.enabled = true;
    }

    for (var idx in forms) {
      if (forms[idx]) {
        if (forms[idx].options) {
          if (config.runOnSelectionChange === undefined) {
            config.runOnSelectionChange = true;
          }
        }
      }
    }

    if (!config.results) {
      config.results = {};
    }

    if (!config.editorSetting) {
      config.editorSetting = {};
    } else if (config.editorSetting.editOnDblClick) {
      editorSetting.isOutputHidden = config.editorSetting.editOnDblClick;
    }
  };

  $scope.getIframeDimensions = function() {
    if ($scope.asIframe) {
      var paragraphid = '#' + $routeParams.paragraphId + '_container';
      var height = angular.element(paragraphid).height();
      return height;
    }
    return 0;
  };

  $scope.$watch($scope.getIframeDimensions, function(newValue, oldValue) {
    if ($scope.asIframe && newValue) {
      var message = {};
      message.height = newValue;
      message.url = $location.$$absUrl;
      $window.parent.postMessage(angular.toJson(message), '*');
    }
  });

  $scope.isRunning = function(paragraph) {
    return paragraph.status === 'RUNNING' || paragraph.status === 'PENDING';
  };

  $scope.runParagraph = function(data) {
    var submitFn;
    $scope.paragraph.config.enabled = false;
    if ($scope.paragraph.id === 'add_rule') {
      submitFn = restapi.submitRule;
    } else if ($scope.paragraph.id === 'run_action') {
      submitFn = restapi.submitAction;
    }
    submitFn($scope.paragraph.text, submit);
  };

  $scope.toggleEnableDisable = function(paragraph) {
    paragraph.config.enabled = !paragraph.config.enabled;
  };

  $scope.run = function(paragraph, editorValue) {
    if (editorValue && !$scope.isRunning(paragraph)) {
      $scope.runParagraph(editorValue);
    }
  };

  $scope.clearParagraphOutput = function(paragraph) {
    $scope.paragraph.results = null;
  };

  $scope.toggleEditor = function(paragraph) {
    if (paragraph.config.editorHide) {
      $scope.openEditor(paragraph);
    } else {
      $scope.closeEditor(paragraph);
    }
  };

  $scope.closeEditor = function(paragraph) {
    console.log('close the note');
    paragraph.config.editorHide = true;
  };

  $scope.openEditor = function(paragraph) {
    console.log('open the note');
    paragraph.config.editorHide = false;
  };

  $scope.closeTable = function(paragraph) {
    console.log('close the output');
    paragraph.config.tableHide = true;
  };

  $scope.openTable = function(paragraph) {
    console.log('open the output');
    paragraph.config.tableHide = false;
  };

  var openEditorAndCloseTable = function(paragraph) {
    manageEditorAndTableState(paragraph, false, true);
  };

  var manageEditorAndTableState = function(paragraph, hideEditor, hideTable) {
    paragraph.config.editorHide = hideEditor;
    paragraph.config.tableHide = hideTable;
  };

  $scope.showLineNumbers = function(paragraph) {
    if ($scope.editor) {
      paragraph.config.lineNumbers = true;
      $scope.editor.renderer.setShowGutter(true);
    }
  };

  $scope.hideLineNumbers = function(paragraph) {
    if ($scope.editor) {
      paragraph.config.lineNumbers = false;
      $scope.editor.renderer.setShowGutter(false);
    }
  };

  $scope.columnWidthClass = function(n) {
    if ($scope.asIframe) {
      return 'col-md-12';
    } else {
      return 'paragraph-col col-md-' + n;
    }
  };

  $scope.toggleOutput = function(paragraph) {
    paragraph.config.tableHide = !paragraph.config.tableHide;
  };

  $scope.loadForm = function(formulaire, params) {
    var value = formulaire.defaultValue;
    if (params[formulaire.name]) {
      value = params[formulaire.name];
    }

    $scope.paragraph.settings.params[formulaire.name] = value;
  };

  $scope.toggleCheckbox = function(formulaire, option) {
    var idx = $scope.paragraph.settings.params[formulaire.name].indexOf(option.value);
    if (idx > -1) {
      $scope.paragraph.settings.params[formulaire.name].splice(idx, 1);
    } else {
      $scope.paragraph.settings.params[formulaire.name].push(option.value);
    }
  };

  $scope.aceChanged = function(_, editor) {
    var session = editor.getSession();
    var dirtyText = session.getValue();
    setParagraphMode(session, dirtyText, editor.getCursorPosition());
  };

  $scope.aceLoaded = function(_editor) {
    var langTools = ace.require('ace/ext/language_tools');
    var Range = ace.require('ace/range').Range;

    _editor.$blockScrolling = Infinity;
    $scope.editor = _editor;
    $scope.editor.on('input', $scope.aceChanged);
    if (_editor.container.id !== '{{paragraph.id}}_editor') {
      $scope.editor.renderer.setShowGutter($scope.paragraph.config.lineNumbers);
      $scope.editor.setShowFoldWidgets(false);
      $scope.editor.setHighlightActiveLine(false);
      $scope.editor.setHighlightGutterLine(false);
      $scope.editor.getSession().setUseWrapMode(true);
      $scope.editor.setTheme('ace/theme/chrome');
      $scope.editor.setReadOnly($scope.isRunning($scope.paragraph));
      if ($scope.paragraphFocused) {
        $scope.editor.focus();
        $scope.goToEnd($scope.editor);
      }

      autoAdjustEditorHeight(_editor);
      angular.element(window).resize(function() {
        autoAdjustEditorHeight(_editor);
      });

      if (navigator.appVersion.indexOf('Mac') !== -1) {
        $scope.editor.setKeyboardHandler('ace/keyboard/emacs');
        $rootScope.isMac = true;
      } else if (navigator.appVersion.indexOf('Win') !== -1 ||
        navigator.appVersion.indexOf('X11') !== -1 ||
        navigator.appVersion.indexOf('Linux') !== -1) {
        $rootScope.isMac = false;
        // not applying emacs key binding while the binding override Ctrl-v. default behavior of paste text on windows.
      }

      $scope.editor.setOptions({
        enableBasicAutocompletion: true,
        enableSnippets: false,
        enableLiveAutocompletion: false
      });

      $scope.editor.on('focus', function() {
        handleFocus(true);
      });

      $scope.editor.on('blur', function() {
        handleFocus(false);
      });

      $scope.editor.on('paste', function(e) {
        if (e.text.indexOf('%') === 0) {
          pastePercentSign = true;
        }
      });

      $scope.editor.getSession().on('change', function(e, editSession) {
        $scope.paragraph.status = 'READY';
        $scope.paragraph.results = null;
        $scope.paragraph.config.enabled = true;
        autoAdjustEditorHeight(_editor);
      });

      setParagraphMode($scope.editor.getSession(), $scope.editor.getSession().getValue());

      // remove binding
      $scope.editor.commands.bindKey('ctrl-alt-n.', null);
      $scope.editor.commands.removeCommand('showSettingsMenu');

      $scope.editor.commands.bindKey('ctrl-alt-l', null);
      $scope.editor.commands.bindKey('ctrl-alt-w', null);
      $scope.editor.commands.bindKey('ctrl-alt-a', null);
      $scope.editor.commands.bindKey('ctrl-alt-k', null);
      $scope.editor.commands.bindKey('ctrl-alt-e', null);
      $scope.editor.commands.bindKey('ctrl-alt-t', null);

      // autocomplete on 'ctrl+.'
      $scope.editor.commands.bindKey('ctrl-.', 'startAutocomplete');
      $scope.editor.commands.bindKey('ctrl-space', null);

      var keyBindingEditorFocusAction = function(scrollValue) {
        var numRows = $scope.editor.getSession().getLength();
        var currentRow = $scope.editor.getCursorPosition().row;
        if (currentRow === 0 && scrollValue <= 0) {
          // move focus to previous paragraph
          $scope.$emit('moveFocusToPreviousParagraph', $scope.paragraph.id);
        } else if (currentRow === numRows - 1 && scrollValue >= 0) {
          $scope.$emit('moveFocusToNextParagraph', $scope.paragraph.id);
        } else {
          $scope.scrollToCursor($scope.paragraph.id, scrollValue);
        }
      };

      // handle cursor moves
      $scope.editor.keyBinding.origOnCommandKey = $scope.editor.keyBinding.onCommandKey;
      $scope.editor.keyBinding.onCommandKey = function(e, hashId, keyCode) {
        if ($scope.editor.completer && $scope.editor.completer.activated) { // if autocompleter is active
        } else {
          // fix ace editor focus issue in chrome (textarea element goes to top: -1000px after focused by cursor move)
          if (parseInt(angular.element('#' + $scope.paragraph.id + '_editor > textarea')
              .css('top').replace('px', '')) < 0) {
            var position = $scope.editor.getCursorPosition();
            var cursorPos = $scope.editor.renderer.$cursorLayer.getPixelPosition(position, true);
            angular.element('#' + $scope.paragraph.id + '_editor > textarea').css('top', cursorPos.top);
          }

          var ROW_UP = -1;
          var ROW_DOWN = 1;

          switch (keyCode) {
            case 38:
              keyBindingEditorFocusAction(ROW_UP);
              break;
            case 80:
              if (e.ctrlKey && !e.altKey) {
                keyBindingEditorFocusAction(ROW_UP);
              }
              break;
            case 40:
              keyBindingEditorFocusAction(ROW_DOWN);
              break;
            case 78:
              if (e.ctrlKey && !e.altKey) {
                keyBindingEditorFocusAction(ROW_DOWN);
              }
              break;
          }
        }
        this.origOnCommandKey(e, hashId, keyCode);
      };
    }
  };

  var handleFocus = function(value, isDigestPass) {
    $scope.paragraphFocused = value;
    if (isDigestPass === false || isDigestPass === undefined) {
      // Protect against error in case digest is already running
      $timeout(function() {
        // Apply changes since they come from 3rd party library
        $scope.$digest();
      });
    }
  };

  var getEditorSetting = function(paragraph, interpreterName) {
    var deferred = $q.defer();
    $timeout(
      $scope.$on('editorSetting', function(event, data) {
          if (paragraph.id === data.paragraphId) {
            deferred.resolve(data);
          }
        }
      ), 1000);
    return deferred.promise;
  };

  var setEditorLanguage = function(session, language) {
    var mode = 'ace/mode/';
    mode += language;
    $scope.paragraph.config.editorMode = mode;
    session.setMode(mode);
  };

  var setParagraphMode = function(session, paragraphText, pos) {
    // Evaluate the mode only if the the position is undefined
    // or the first 30 characters of the paragraph have been modified
    // or cursor position is at beginning of second line.(in case user hit enter after typing %magic)
    if ((typeof pos === 'undefined') || (pos.row === 0 && pos.column < 30) ||
      (pos.row === 1 && pos.column === 0) || pastePercentSign) {
      // If paragraph loading, use config value if exists
      if ((typeof pos === 'undefined') && $scope.paragraph.config.editorMode &&
        !setInterpreterBindings) {
        session.setMode($scope.paragraph.config.editorMode);
      } else {
        var magic = getInterpreterName(paragraphText);
        if (editorSetting.magic !== magic) {
          editorSetting.magic = magic;
          getEditorSetting($scope.paragraph, magic)
            .then(function(setting) {
              setEditorLanguage(session, setting.editor.language);
              _.merge($scope.paragraph.config.editorSetting, setting.editor);
            });
        }
      }
    }
    pastePercentSign = false;
    setInterpreterBindings = false;
  };

  var getInterpreterName = function(paragraphText) {
    var intpNameRegexp = /^\s*%(.+?)\s/g;
    var match = intpNameRegexp.exec(paragraphText);
    if (match) {
      return match[1].trim();
      // get default interpreter name if paragraph text doesn't start with '%'
      // TODO(mina): dig into the cause what makes interpreterBindings to have no element
    } else if ($scope.$parent.interpreterBindings && $scope.$parent.interpreterBindings.length !== 0) {
      return $scope.$parent.interpreterBindings[0].name;
    }
    return '';
  };

  var autoAdjustEditorHeight = function(editor) {
    var height =
      editor.getSession().getScreenLength() *
      editor.renderer.lineHeight +
      editor.renderer.scrollBar.getWidth();

    angular.element('#' + editor.container.id).height(height.toString() + 'px');
    editor.resize();
  };

  $rootScope.$on('scrollToCursor', function(event) {
    // scroll on 'scrollToCursor' event only when cursor is in the last paragraph
    var paragraphs = angular.element('div[id$="_paragraphColumn_main"]');
    if (paragraphs[paragraphs.length - 1].id.indexOf($scope.paragraph.id) === 0) {
      $scope.scrollToCursor($scope.paragraph.id, 0);
    }
  });

  /** scrollToCursor if it is necessary
   * when cursor touches scrollTriggerEdgeMargin from the top (or bottom) of the screen, it autoscroll to place cursor around 1/3 of screen height from the top (or bottom)
   * paragraphId : paragraph that has active cursor
   * lastCursorMove : 1(down), 0, -1(up) last cursor move event
   **/
  $scope.scrollToCursor = function(paragraphId, lastCursorMove) {
    if (!$scope.editor || !$scope.editor.isFocused()) {
      // only make sense when editor is focused
      return;
    }
    var lineHeight = $scope.editor.renderer.lineHeight;
    var headerHeight = 103; // menubar, notebook titlebar
    var scrollTriggerEdgeMargin = 50;

    var documentHeight = angular.element(document).height();
    var windowHeight = angular.element(window).height();  // actual viewport height

    var scrollPosition = angular.element(document).scrollTop();
    var editorPosition = angular.element('#' + paragraphId + '_editor').offset();
    var position = $scope.editor.getCursorPosition();
    var lastCursorPosition = $scope.editor.renderer.$cursorLayer.getPixelPosition(position, true);

    var calculatedCursorPosition = editorPosition.top + lastCursorPosition.top + lineHeight * lastCursorMove;

    var scrollTargetPos;
    if (calculatedCursorPosition < scrollPosition + headerHeight + scrollTriggerEdgeMargin) {
      scrollTargetPos = calculatedCursorPosition - headerHeight - ((windowHeight - headerHeight) / 3);
      if (scrollTargetPos < 0) {
        scrollTargetPos = 0;
      }
    } else if (calculatedCursorPosition > scrollPosition + scrollTriggerEdgeMargin + windowHeight - headerHeight) {
      scrollTargetPos = calculatedCursorPosition - headerHeight - ((windowHeight - headerHeight) * 2 / 3);

      if (scrollTargetPos > documentHeight) {
        scrollTargetPos = documentHeight;
      }
    }

    // cancel previous scroll animation
    var bodyEl = angular.element('body');
    bodyEl.stop();
    bodyEl.finish();

    // scroll to scrollTargetPos
    bodyEl.scrollTo(scrollTargetPos, {axis: 'y', interrupt: true, duration: 100});
  };

  $scope.getEditorValue = function() {
    return !$scope.editor ? $scope.paragraph.text : $scope.editor.getValue();
  };

  $scope.getProgress = function() {
    return $scope.currentProgress || 0;
  };

  $scope.getExecutionTime = function(pdata) {
    var timeMs = Date.parse(pdata.dateFinished) - Date.parse(pdata.dateStarted);
    if (isNaN(timeMs) || timeMs < 0) {
      if ($scope.isResultOutdated(pdata)) {
        return 'outdated';
      }
      return '';
    }
    var user = (pdata.user === undefined || pdata.user === null) ? 'anonymous' : pdata.user;
    var desc = 'Took ' + moment.duration((timeMs / 1000), 'seconds').format('h [hrs] m [min] s [sec]') +
      '. Last updated by ' + user + ' at ' + moment(pdata.dateFinished).format('MMMM DD YYYY, h:mm:ss A') + '.';
    if ($scope.isResultOutdated(pdata)) {
      desc += ' (outdated)';
    }
    return desc;
  };

  $scope.getElapsedTime = function(paragraph) {
    return 'Started ' + moment(paragraph.dateStarted).fromNow() + '.';
  };

  $scope.isResultOutdated = function(pdata) {
    if (pdata.dateUpdated !== undefined && Date.parse(pdata.dateUpdated) > Date.parse(pdata.dateStarted)) {
      return true;
    }
    return false;
  };

  $scope.goToEnd = function(editor) {
    editor.navigateFileEnd();
  };

  $scope.getResultType = function(paragraph) {
    var pdata = (paragraph) ? paragraph : $scope.paragraph;
    if (pdata.results && pdata.results.type) {
      return pdata.results.type;
    } else {
      return 'TEXT';
    }
  };

  $scope.parseTableCell = function(cell) {
    if (!isNaN(cell)) {
      if (cell.length === 0 || Number(cell) > Number.MAX_SAFE_INTEGER || Number(cell) < Number.MIN_SAFE_INTEGER) {
        return cell;
      } else {
        return Number(cell);
      }
    }
    var d = moment(cell);
    if (d.isValid()) {
      return d;
    }
    return cell;
  };

  /** Utility function */
  $scope.goToSingleParagraph = function() {
    var noteId = $route.current.pathParams.noteId;
    var redirectToUrl = location.protocol + '//' + location.host + location.pathname + '#/notebook/' + noteId +
      '/paragraph/' + $scope.paragraph.id + '?asIframe';
    $window.open(redirectToUrl);
  };

  $scope.showScrollDownIcon = function(id) {
    var doc = angular.element('#p' + id + '_text');
    if (doc[0]) {
      return doc[0].scrollHeight > doc.innerHeight();
    }
    return false;
  };

  $scope.scrollParagraphDown = function(id) {
    var doc = angular.element('#p' + id + '_text');
    doc.animate({scrollTop: doc[0].scrollHeight}, 500);
    $scope.keepScrollDown = true;
  };

  $scope.showScrollUpIcon = function(id) {
    if (angular.element('#p' + id + '_text')[0]) {
      return angular.element('#p' + id + '_text')[0].scrollTop !== 0;
    }
    return false;
  };

  $scope.$on('updateProgress', function(event, data) {
    if (data.id === $scope.paragraph.id) {
      $scope.currentProgress = data.progress;
    }
  });

  $scope.$on('keyEvent', function(event, keyEvent) {
    if ($scope.paragraphFocused) {

      var paragraphId = $scope.paragraph.id;
      var keyCode = keyEvent.keyCode;
      var noShortcutDefined = false;
      var editorHide = $scope.paragraph.config.editorHide;

      if (editorHide && (keyCode === 38 || (keyCode === 80 && keyEvent.ctrlKey && !keyEvent.altKey))) { // up
        // move focus to previous paragraph
        $scope.$emit('moveFocusToPreviousParagraph', paragraphId);
      } else if (keyEvent.shiftKey && keyCode === 13) { // Shift + Enter
        $scope.run($scope.paragraph, $scope.getEditorValue());
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 79) { // Ctrl + Alt + o
        $scope.toggleOutput($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 82) { // Ctrl + Alt + r
        $scope.toggleEnableDisable($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 69) { // Ctrl + Alt + e
        $scope.toggleEditor($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 77) { // Ctrl + Alt + m
        if ($scope.paragraph.config.lineNumbers) {
          $scope.hideLineNumbers($scope.paragraph);
        } else {
          $scope.showLineNumbers($scope.paragraph);
        }
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 189) { // Ctrl + Shift + -
        $scope.changeColWidth($scope.paragraph, Math.max(1, $scope.paragraph.config.colWidth - 1));
      } else if (keyEvent.ctrlKey && keyEvent.shiftKey && keyCode === 187) { // Ctrl + Shift + =
        $scope.changeColWidth($scope.paragraph, Math.min(12, $scope.paragraph.config.colWidth + 1));
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 84) { // Ctrl + Alt + t
        if ($scope.paragraph.config.title) {
          $scope.hideTitle($scope.paragraph);
        } else {
          $scope.showTitle($scope.paragraph);
        }
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 76) { // Ctrl + Alt + l
        $scope.clearParagraphOutput($scope.paragraph);
      } else if (keyEvent.ctrlKey && keyEvent.altKey && keyCode === 87) { // Ctrl + Alt + w
        $scope.goToSingleParagraph();
      } else {
        noShortcutDefined = true;
      }

      if (!noShortcutDefined) {
        keyEvent.preventDefault();
      }
    }
  });

  $scope.$on('focusParagraph', function(event, paragraphId, cursorPos, mouseEvent) {
    if ($scope.paragraph.id === paragraphId) {
      // focus editor
      if (!$scope.paragraph.config.editorHide) {
        if (!mouseEvent) {
          $scope.editor.focus();
          // move cursor to the first row (or the last row)
          var row;
          if (cursorPos >= 0) {
            row = cursorPos;
            $scope.editor.gotoLine(row, 0);
          } else {
            row = $scope.editor.session.getLength();
            $scope.editor.gotoLine(row, 0);
          }
          $scope.scrollToCursor($scope.paragraph.id, 0);
        }
      }
      handleFocus(true);
    } else {
      if ($scope.editor !== undefined && $scope.editor !== null) {
        $scope.editor.blur();
      }
      var isDigestPass = true;
      handleFocus(false, isDigestPass);
    }
  });

  $scope.$on('doubleClickParagraph', function(event, paragraphId) {
    if ($scope.paragraph.id === paragraphId && $scope.paragraph.config.editorHide &&
      $scope.paragraph.config.editorSetting.editOnDblClick && $scope.revisionView !== true) {
      var deferred = $q.defer();
      openEditorAndCloseTable($scope.paragraph);
      $timeout(
        $scope.$on('updateParagraph', function(event, data) {
            deferred.resolve(data);
          }
        ), 1000);

      deferred.promise.then(function(data) {
        if ($scope.editor) {
          $scope.editor.focus();
          $scope.goToEnd($scope.editor);
        }
      });
    }
  });

  $scope.$on('openEditor', function(event) {
    $scope.openEditor($scope.paragraph);
  });

  $scope.$on('closeEditor', function(event) {
    $scope.closeEditor($scope.paragraph);
  });

  $scope.$on('openTable', function(event) {
    $scope.openTable($scope.paragraph);
  });

  $scope.$on('closeTable', function(event) {
    $scope.closeTable($scope.paragraph);
  });
}
