/*! dashing (assembled widgets) v0.4.9 | Apache License 2.0 | https://github.com/stanleyxu2005/dashing */
(function(window, document, undefined) {
    'use strict';
    angular.module('dashing', [
        'dashing.progressbar'
    ]);
    angular.module('dashing').run(['$templateCache',
        function($templateCache) {
            $templateCache.put('progressbar/progressbar.html', '<div style="width: 100%">  <span class="small pull-left" ng-bind="current+\'/\'+max"></span> <span class="small pull-right" ng-bind="usage + \'%\'"></span> </div> <div style="clear: both; height: 1px"></div> <div style="width: 100%" class="progress progress-tiny"> <div ng-style="{\'width\': usage+\'%\'}" class="progress-bar {{usageClass}}"></div> </div>');
        }
    ]);
    angular.module('dashing.util.bootstrap', [])
        .factory('dashing.util.bootstrap', function() {
            return {
                conditionToBootstrapLabelClass: function(condition) {
                    switch (condition) {
                        case 'good':
                            return 'label-success';
                        case 'concern':
                            return 'label-warning';
                        case 'danger':
                            return 'label-danger';
                        default:
                            return 'label-default';
                    }
                }
            };
        });
    angular.module('dashing.progressbar', [])
        .directive('progressbar', function() {
            return {
                restrict: 'E',
                templateUrl: 'progressbar/progressbar.html',
                scope: {
                    current: '@',
                    max: '@',
                    colorMapperFn: '='
                },
                link: function(scope, elem, attrs) {
                    attrs.$observe('current', function(current) {
                        updateUsageAndClass(Number(current), Number(attrs.max));
                    });
                    attrs.$observe('max', function(max) {
                        updateUsageAndClass(Number(attrs.current), Number(max));
                    });
                    function updateUsageAndClass(current, max) {
                        scope.usage = max > 0 ? Math.round(current * 100 / max) : -1;
                        scope.usageClass = (scope.colorMapperFn ?
                            scope.colorMapperFn : defaultColorMapperFn)(scope.usage);
                    }
                    function defaultColorMapperFn(usage) {
                        return 'progress-bar-' +
                            (usage < 50 ? 'info' : (usage < 75 ? 'warning' : 'danger'));
                    }
                }
            };
        });
    angular.module('dashing.tables.property_table.builder', [])
        .factory('$propertyTableBuilder', ['dsPropertyRenderer',
            function(renderer) {
                var PB = function(renderer, title) {
                    this.props = renderer ? {
                            renderer: renderer
                        } : {};
                    if (title) {
                        this.title(title);
                    }
                };
                PB.prototype.title = function(title) {
                    this.props.name = title;
                    return this;
                };
                PB.prototype.help = function(help) {
                    this.props.help = help;
                    return this;
                };
                PB.prototype.value = function(value) {
                    this.props.value = value;
                    return this;
                };
                PB.prototype.values = function(values) {
                    if (!Array.isArray(values)) {
                        console.warn('values must be an array');
                        values = [values];
                    }
                    this.props.values = values;
                    return this;
                };
                PB.prototype.done = function() {
                    return this.props;
                };
                return {
                    button: function(title) {
                        return new PB(renderer.BUTTON, title);
                    },
                    bytes: function(title) {
                        console.warn('deprecated: should use number() instead');
                        return new PB(renderer.BYTES, title);
                    },
                    datetime: function(title) {
                        return new PB(renderer.DATETIME, title);
                    },
                    duration: function(title) {
                        return new PB(renderer.DURATION, title);
                    },
                    indicator: function(title) {
                        return new PB(renderer.INDICATOR, title);
                    },
                    link: function(title) {
                        return new PB(renderer.LINK, title);
                    },
                    number: function(title) {
                        return new PB(renderer.NUMBER, title);
                    },
                    number1: function(title) {
                        return new PB(renderer.NUMBER1, title);
                    },
                    number2: function(title) {
                        return new PB(renderer.NUMBER2, title);
                    },
                    progressbar: function(title) {
                        return new PB(renderer.PROGRESS_BAR, title);
                    },
                    tag: function(title) {
                        return new PB(renderer.TAG, title);
                    },
                    text: function(title) {
                        return new PB(renderer.TEXT, title);
                    },
                    $update: function(target, values) {
                        angular.forEach(values, function(value, i) {
                            var field = Array.isArray(value) ? 'values' : 'value';
                            target[i][field] = value;
                        });
                        return target;
                    }
                };
            }
        ]);
    angular.module('dashing.tables.sortable_table', [
        'smart-table',
        'dashing.property',
        'dashing.util'
    ])
        .directive('sortableTable', ['dsPropertyRenderer', 'dashing.util', 'dashing.i18n',
            function(renderer, util, i18n) {
                return {
                    restrict: 'E',
                    templateUrl: 'tables/sortable_table/sortable_table.html',
                    transclude: true,
                    scope: {
                        caption: '@',
                        captionTooltip: '@',
                        subCaption: '@',
                        pagination: '@',
                        columns: '=columnsBind',
                        records: '=recordsBind',
                        search: '=searchBind',
                        emptySearchResult: '@'
                    },
                    link: function(scope, elem) {
                        var searchControl = elem.find('input')[0];
                        scope.$watch('search', function(val) {
                            searchControl.value = val || '';
                            angular.element(searchControl).triggerHandler('input');
                        });
                        scope.emptySearchResult = scope.emptySearchResult || i18n.emptySearchResult;
                        scope.$watch('columns', function(columns) {
                            if (!Array.isArray(columns)) {
                                console.warn('Failed to create table, until columns are defined.');
                                return;
                            }
                            scope.columnStyleClass = columns.map(function(column) {
                                function addStyleClass(dest, clazz, condition) {
                                    if (condition) {
                                        dest.push(clazz);
                                    }
                                }
                                var array = [];
                                addStyleClass(array, column.styleClassArray.join(' '), column.styleClassArray.length);
                                addStyleClass(array, 'text-nowrap', Array.isArray(column.key) && !column.vertical);
                                return array.join(' ');
                            });
                            var possibleStripeColumns = columns.map(function(column) {
                                if (!Array.isArray(column.key) && column.renderer === renderer.INDICATOR) {
                                    return column.key;
                                }
                            });
                            scope.bgColorForStripeFix = function(index, record) {
                                var key = possibleStripeColumns[index];
                                if (key) {
                                    var cell = record[key];
                                    if (cell.shape === 'stripe') {
                                        return util.color.conditionToColor(cell.condition);
                                    }
                                }
                            };
                            scope.multipleRendererColumnsRenderers = columns.map(function(column) {
                                if (!Array.isArray(column.key)) {
                                    return null;
                                }
                                if (Array.isArray(column.renderer)) {
                                    if (column.renderer.length !== column.key.length) {
                                        console.warn('Every column key should have a renderer, or share one renderer.');
                                    }
                                    return column.renderer;
                                }
                                return column.key.map(function() {
                                    return column.renderer;
                                });
                            });
                        });
                        scope.hasRecords = function() {
                            return scope.search || (Array.isArray(scope.records) && scope.records.length > 0);
                        };
                        scope.isArray = Array.isArray;
                    }
                };
            }
        ])
        .directive('stSummary', ['dashing.i18n',
            function(i18n) {
                return {
                    require: '^stTable',
                    template: i18n.paginationSummary,
                    link: function(scope, element, attrs, stTable) {
                        scope.stRange = {
                            from: null,
                            to: null
                        };
                        scope.$watch('currentPage', updateText);
                        scope.$watch('totalItemCount', updateText);
                        function updateText() {
                            var pagination = stTable.tableState().pagination;
                            if (pagination.totalItemCount === 0) {
                                scope.stRange.from = 0;
                                scope.stRange.to = 0;
                            } else {
                                scope.stRange.from = pagination.start + 1;
                                scope.stRange.to = scope.currentPage === pagination.numberOfPages ?
                                    pagination.totalItemCount : (scope.stRange.from + scope.stItemsByPage - 1);
                            }
                        }
                    }
                };
            }
        ])
        .config(['stConfig',
            function(stConfig) {
                stConfig.sort.skipNatural = true;
                stConfig.sort.delay = -1;
            }
        ]);
    angular.module('dashing.tables.sortable_table.builder', [
        'dashing.property',
        'dashing.util'
    ])
        .factory('$sortableTableBuilder', ['dashing.util', 'dsPropertyRenderer',
            function(util, renderer) {
                var CB = function(renderer, title) {
                    this.props = renderer ? {
                            renderer: renderer
                        } : {};
                    this.props.styleClassArray = [];
                    if (title) {
                        this.title(title);
                    }
                };
                CB.prototype.title = function(title) {
                    this.props.name = title;
                    return this;
                };
                CB.prototype.key = function(key) {
                    this.props.key = key;
                    return this;
                };
                CB.prototype.canSort = function(overrideSortKey) {
                    if (!overrideSortKey) {
                        if (!this.props.key) {
                            console.warn('The column does not have a key. Call `.key("some")` first!');
                            return;
                        } else if (Array.isArray(this.props.key)) {
                            console.warn('Multiple keys found. We use the first key for sorting by default.');
                            overrideSortKey = this.props.key[0];
                        }
                    }
                    this.props.sortKey = overrideSortKey || this.props.key;
                    if (this.props.sortKey === this.props.key) {
                        switch (this.props.renderer) {
                            case renderer.LINK:
                                this.props.sortKey += '.text';
                                break;
                            case renderer.INDICATOR:
                            case renderer.TAG:
                                this.props.sortKey += '.condition';
                                break;
                            case renderer.PROGRESS_BAR:
                                this.props.sortKey += '.usage';
                                break;
                            case renderer.BYTES:
                                this.props.sortKey += '.raw';
                                break;
                            case renderer.BUTTON:
                                console.warn('"%s" column is not sortable.');
                                return;
                            default:
                        }
                    }
                    return this;
                };
                CB.prototype.sortDefault = function(descent) {
                    if (!this.props.sortKey) {
                        console.warn('Specify a sort key or define column key first!');
                        return;
                    }
                    this.props.defaultSort = descent ? 'reverse' : true;
                    return this;
                };
                CB.prototype.sortDefaultDescent = function() {
                    return this.sortDefault(true);
                };
                CB.prototype.styleClass = function(styleClass) {
                    var styles = styleClass.split(' ');
                    angular.forEach(styles, function(style) {
                        if (this.props.styleClassArray.indexOf(style) === -1) {
                            this.props.styleClassArray.push(style);
                        }
                    }, this);
                    return this;
                };
                CB.prototype.textRight = function() {
                    this.styleClass('text-right');
                    return this;
                };
                CB.prototype.textLeft = function() {
                    var i = this.props.styleClassArray.indexOf('text-right');
                    if (i !== -1) {
                        this.props.styleClassArray.splice(i, 1);
                    }
                    return this;
                };
                CB.prototype.sortBy = function(sortKey) {
                    this.props.sortKey = sortKey;
                    return this;
                };
                CB.prototype.unit = function(unit) {
                    this.props.unit = unit;
                    return this;
                };
                CB.prototype.help = function(help) {
                    this.props.help = help;
                    return this;
                };
                CB.prototype.vertical = function() {
                    if (Array.isArray(this.props.key)) {
                        this.props.vertical = true;
                    }
                    return this;
                };
                CB.prototype.done = function() {
                    return this.props;
                };
                function arrayKeyEqual(lhs, rhs, key) {
                    var equal = true;
                    angular.forEach(rhs, function(value, i) {
                        var one = lhs[i];
                        if (!one.hasOwnProperty(key) || one.key !== rhs.key) {
                            equal = false;
                            return false;
                        }
                    });
                    return equal;
                }
                return {
                    button: function(title) {
                        return new CB(renderer.BUTTON, title);
                    },
                    bytes: function(title) {
                        console.warn('deprecated: should use number instead');
                        return (new CB(renderer.BYTES, title)).textRight();
                    },
                    datetime: function(title) {
                        return new CB(renderer.DATETIME, title);
                    },
                    duration: function(title) {
                        return new CB(renderer.DURATION, title);
                    },
                    indicator: function(title) {
                        return new CB(renderer.INDICATOR, title);
                    },
                    link: function(title) {
                        return new CB(renderer.LINK, title);
                    },
                    multiple: function(title, renderers) {
                        return new CB(renderers, title);
                    },
                    number: function(title) {
                        return (new CB(renderer.NUMBER, title)).textRight();
                    },
                    number1: function(title) {
                        return (new CB(renderer.NUMBER1, title)).textRight();
                    },
                    number2: function(title) {
                        return (new CB(renderer.NUMBER2, title)).textRight();
                    },
                    progressbar: function(title) {
                        return new CB(renderer.PROGRESS_BAR, title);
                    },
                    tag: function(title) {
                        return new CB(renderer.TAG, title);
                    },
                    text: function(title) {
                        return new CB(renderer.TEXT, title);
                    },
                    $update: function(target, values, keyToCheck) {
                        if ((target || []).length !== (values || []).length) {
                            return values;
                        }
                        if (angular.isString(keyToCheck) && !arrayKeyEqual(target, values, keyToCheck)) {
                            return values;
                        }
                        angular.forEach(values, function(value, i) {
                            target[i] = value;
                        });
                        return target;
                    },
                    $check: function(cols, model) {
                        angular.forEach(cols, function(col) {
                            var keys = util.array.ensureArray(col.key);
                            angular.forEach(keys, function(key) {
                                if (!model.hasOwnProperty(key)) {
                                    console.warn('Model does not have a property matches column key `' + col + '`.');
                                }
                            });
                        });
                    }
                };
            }
        ]);
    angular.module('dashing.util', [
        'dashing.util.array',
        'dashing.util.bootstrap',
        'dashing.util.color',
        'dashing.util.text',
        'dashing.util.validation'
    ])
        .factory('dashing.util', [
            'dashing.util.array',
            'dashing.util.bootstrap',
            'dashing.util.color',
            'dashing.util.text',
            'dashing.util.validation',
            function(array, bootstrap, color, text, validation) {
                return {
                    array: array,
                    bootstrap: bootstrap,
                    color: color,
                    text: text,
                    validation: validation
                };
            }
        ]);
    angular.module('dashing.util.validation', [])
        .factory('dashing.util.validation', function() {
            var self = {
                class: function(s) {
                    return /^[a-zA-Z_][a-zA-Z_\d]*(\.[a-zA-Z_][a-zA-Z_\d]*)*$/i.test(s);
                },
                integer: function(s) {
                    return /^-?\d+$/.test(s);
                },
                integerInRange: function(s, min, max) {
                    if (self.integer(s)) {
                        s = Number(s);
                        return (isNaN(min) || (s >= min)) && (isNaN(max) || (s <= max));
                    }
                    return false;
                }
            };
            return self;
        });
})(window, document);
