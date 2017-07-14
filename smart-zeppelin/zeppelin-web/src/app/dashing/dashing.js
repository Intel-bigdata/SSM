/*! dashing (assembled widgets) v0.4.9 | Apache License 2.0 | https://github.com/stanleyxu2005/dashing */
(function(window, document, undefined) {
'use strict';
angular.module('dashing', [
  'dashing.charts.bar',
  'dashing.charts.line',
  'dashing.charts.metrics_sparkline',
  'dashing.charts.ring',
  'dashing.charts.sparkline',
  'dashing.dialogs',
  'dashing.forms.form_control',
  'dashing.forms.searchbox',
  'dashing.metrics',
  'dashing.progressbar',
  'dashing.property',
  'dashing.property.number',
  'dashing.remark',
  'dashing.state.indicator',
  'dashing.state.tag',
  'dashing.tables.property_table',
  'dashing.tables.sortable_table',
  'dashing.tabset',
  'dashing.contextmenu',
  'dashing.tables.property_table.builder',
  'dashing.tables.sortable_table.builder',
  'dashing.filters.any',
  'dashing.filters.duration'
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
angular.module('dashing.charts.adapter.echarts', [
  'dashing.util'
])
  .directive('echart', ['EchartWrapper',
    function(EchartWrapper) {
      return {
        restrict: 'E',
        template: '<div></div>',
        replace: true,
        scope: {
          options: '=',
          api: '=',
          onResize: '&'
        },
        controller: ['$scope', '$element', 'dashing.charts.echarts.defaults',
          function($scope, $element, defaults) {
            var options = $scope.options;
            var elem0 = $element[0];
            angular.forEach(['width', 'height'], function(prop) {
              if (options[prop]) {
                elem0.style[prop] = options[prop];
              }
            });
            var chart = echarts.init(elem0);
            chart.setTheme(defaults.lookAndFeel);
            function handleResizeEvent() {
              var handled = $scope.onResize();
              if (handled) {
                return $scope.$apply();
              }
              chart.resize();
            }
            angular.element(window).on('resize', handleResizeEvent);
            $scope.$on('$destroy', function() {
              angular.element(window).off('resize', handleResizeEvent);
              chart.dispose();
              chart = null;
            });
            $scope.api = new EchartWrapper(chart);
            $scope.api.rebuild(options);
          }
        ]
      };
    }
  ])
  .service('EchartWrapper', ['dashing.util',
    function(util) {
      var EchartWrapper = function(chart) {
        this.chart = chart;
        this.initOptions = null;
      };
      EchartWrapper.prototype = {
        rebuild: function(options) {
          this.chart.hideLoading();
          this.chart.clear();
          this.initOptions = null;
          this.chart.setOption(options, true);
          this.chart.resize();
          if (!this.isGraphDataAvailable()) {
            this.initOptions = angular.copy(options);
            return;
          }
          this.addDataPoints(options.dataPointsQueue, true);
          this._applyGroupingFix(options.groupId);
        },
        _applyGroupingFix: function(groupId) {
          if (angular.isFunction(this.chart.group) && String(groupId).length) {
            this.chart.groupId = groupId;
            this.chart.group();
          }
        },
        addDataPoints: function(dataPoints, silent) {
          if (!Array.isArray(dataPoints) || !dataPoints.length) {
            if (!silent) {
              console.warn({
                msg: 'Invalid input data points',
                data: dataPoints
              });
            }
            return;
          }
          if (this.initOptions !== null) {
            this.rebuild(this.initOptions);
          }
          var currentChartOptions = this.chart.getOption();
          var dataArray = this._dataPointsToDataArray(dataPoints, currentChartOptions);
          if (dataArray.length > 0) {
            this.chart.addData(dataArray);
          }
        },
        _dataPointsToDataArray: function(dataPoints, options) {
          try {
            var actualVisibleDataPoints = options.series[0].data.length;
            var dataPointsGrowNum = Math.max(0, (options.visibleDataPointsNum || Number.MAX_VALUE) - actualVisibleDataPoints);
            var xAxisTypeIsTime = (options.xAxis[0].type === 'time') ||
              (options.xAxis[0].type === 'value' && options.yAxis[0].type === 'time');
            var seriesNum = options.series.length;
            return this._makeDataArray(dataPoints, seriesNum, dataPointsGrowNum, xAxisTypeIsTime);
          } catch (ex) {}
          return [];
        },
        _makeDataArray: function(data, seriesNum, dataPointsGrowNum, xAxisTypeIsTime) {
          var array = [];
          angular.forEach(util.array.ensureArray(data), function(datum) {
            var dataGrow = dataPointsGrowNum-- > 0;
            var yValues = util.array.ensureArray(datum.y).slice(0, seriesNum);
            if (xAxisTypeIsTime) {
              angular.forEach(yValues, function(yValue, seriesIndex) {
                var params = [seriesIndex, [datum.x, yValue], false, dataGrow];
                array.push(params);
              });
            } else {
              var lastSeriesIndex = yValues.length - 1;
              angular.forEach(yValues, function(yValue, seriesIndex) {
                var params = [seriesIndex, yValue, false, dataGrow];
                if (seriesIndex === lastSeriesIndex) {
                  params.push(datum.x);
                }
                array.push(params);
              });
            }
          });
          return array;
        },
        isGraphDataAvailable: function() {
          var currentOptions = this.chart.getOption();
          return angular.isObject(currentOptions.xAxis) &&
            currentOptions.xAxis.length &&
            currentOptions.xAxis[0].data;
        },
        updateOption: function(options) {
          this.chart.setOption(options, false);
        }
      };
      return EchartWrapper;
    }
  ])
  .constant('dashing.charts.echarts.defaults', {
    lookAndFeel: {
      markLine: {
        symbol: ['circle', 'circle']
      },
      title: {
        textStyle: {
          fontSize: 14,
          fontWeight: 400,
          color: '#000'
        }
      },
      legend: {
        textStyle: {
          color: '#111',
          fontWeight: 500
        },
        itemGap: 20
      },
      tooltip: {
        borderRadius: 2,
        padding: 0,
        showDelay: 0,
        transitionDuration: 0.5,
        position: function(pos) {
          return [pos[0], 10];
        }
      },
      textStyle: {
        fontFamily: 'Roboto,"Helvetica Neue","Segoe UI","Hiragino Sans GB","Microsoft YaHei",Arial,Helvetica,SimSun,sans-serif',
        fontSize: 12
      },
      loadingText: 'Data Loading...',
      noDataText: 'No Graphic Data Available',
      addDataAnimation: false
    },
    visibleDataPointsNum: 80
  })
  .factory('$echarts', ['$filter', 'dashing.util',
    function($filter, util) {
      function buildTooltipSeriesTable(name, array, use) {
        function tooltipSeriesColorIndicatorHtml(color) {
          var border = util.color.lighter(color, -0.2);
          return '<div style="width: 10px; height: 10px; margin-top: 2px; border-radius: 2px; border: 1px solid ' + border + '; background-color: ' + color + '"></div>';
        }
        function mergeValuesAndSortByName(array) {
          var grouped = {};
          angular.forEach(array, function(point) {
            grouped[point.name] = grouped[point.name] || [];
            grouped[point.name].push(point);
          });
          var result = [];
          angular.forEach(grouped, function(group) {
            var selected = group.reduce(function(p, c) {
              return Math.abs(Number(p.value)) > Math.abs(c.value) ? p : c;
            });
            selected.value = group.reduce(function(p, c) {
              return {
                value: p.value + c.value
              };
            }).value;
            result.push(selected);
          });
          return result.reverse();
        }
        var valueFormatter = use.valueFormatter || defaultValueFormatter;
        return '<div style="padding: 8px">' + [
          (use.nameFormatter || defaultNameFormatter)(name),
          '<table>' +
          mergeValuesAndSortByName(array).map(function(point) {
            if (point.value === '-') {
              return '';
            } else {
              point.value = valueFormatter(point.value);
            }
            if (!point.name) {
              point.name = point.value;
              point.value = '';
            }
            return '<tr>' +
              '<td>' + tooltipSeriesColorIndicatorHtml(point.color) + '</td>' +
              '<td style="padding: 0 12px 0 4px">' + point.name + '</td>' +
              '<td style="text-align: right">' + point.value + '</td>' +
              '</tr>';
          }).join('') +
          '</table>'
        ].join('') +
          '</div>';
      }
      function defaultNameFormatter(name) {
        if (angular.isDate(name)) {
          var now = new Date();
          return $filter('date')(name, (now.getYear() === name.getYear() &&
              now.getMonth() === name.getMonth() &&
              now.getDay() === name.getDay()) ?
            'HH:mm:ss' : 'yyyy-MM-dd HH:mm:ss');
        }
        return name;
      }
      function defaultValueFormatter(value) {
        return $filter('number')(value);
      }
      function tooltip(args) {
        return {
          trigger: args.trigger || 'axis',
          axisPointer: {
            type: 'none'
          },
          formatter: args.formatter
        };
      }
      function splitInitialData(data, visibleDataPoints) {
        if (!Array.isArray(data)) {
          data = [];
        }
        if (!visibleDataPoints || data.length <= visibleDataPoints) {
          return {
            older: data,
            newer: []
          };
        }
        return {
          older: data.slice(0, visibleDataPoints),
          newer: data.slice(visibleDataPoints)
        };
      }
      return {
        categoryTooltip: function(valueFormatter, nameFormatter) {
          return tooltip({
            trigger: 'axis',
            formatter: function(params) {
              params = util.array.ensureArray(params);
              var name = params[0].name;
              var array = params.map(function(param) {
                return {
                  color: param.series.colors.line,
                  name: param.seriesName,
                  value: param.value
                };
              });
              if (!name.length && !array.filter(function(point) {
                return point.value !== '-';
              }).length) {
                return '';
              }
              var args = {
                nameFormatter: nameFormatter,
                valueFormatter: valueFormatter
              };
              return buildTooltipSeriesTable(name, array, args);
            }
          });
        },
        timelineChartFix: function(options, use) {
          console.warn('Echarts does not have a good experience for time series. ' +
            'We suggest to use category as x-axis type.');
          options.tooltip = tooltip({
            trigger: 'item',
            formatter: function(params) {
              var array = [{
                color: params.series.colors.line,
                name: params.series.name,
                value: params.value[1]
              }];
              return buildTooltipSeriesTable(params.value[0], array, use);
            }
          });
          angular.forEach(options.xAxis, function(axis) {
            delete axis.boundaryGap;
          });
          angular.forEach(options.series, function(series) {
            series.showAllSymbol = true;
            series.stack = false;
          });
        },
        validateSeriesNames: function(use, data) {
          if (!use.seriesNames) {
            var first = util.array.ensureArray(data[0].y);
            if (first.length > 1) {
              console.warn({
                message: 'You should define `options.seriesNames`',
                options: use
              });
            }
            use.seriesNames = first.map(function(_, i) {
              return 'Series ' + (i + 1);
            });
          }
        },
        axisLabelFormatter: function(unit, replaceLookup) {
          return function(value) {
            if (replaceLookup && replaceLookup.hasOwnProperty(value)) {
              return replaceLookup[value];
            }
            if (value != 0 && angular.isNumber(value)) {
              var hr = util.text.toHumanReadableNumber(value, 1000, 1);
              return hr.value + (unit ? ' ' + hr.modifier + unit : hr.modifier.toLowerCase());
            }
            return value;
          };
        },
        makeDataSeries: function(args) {
          var options = {
            type: args.type || 'line',
            symbol: 'circle',
            symbolSize: 4,
            smooth: args.smooth,
            itemStyle: {
              normal: {
                color: args.colors.line,
                lineStyle: {
                  width: args.stack ? 4 : 3
                },
                borderColor: 'transparent',
                borderWidth: 6
              },
              emphasis: {
                color: args.colors.hover,
                borderColor: util.color.alpha(args.colors.line, 0.3)
              }
            }
          };
          if (args.stack) {
            options.itemStyle.normal.areaStyle = {
              type: 'default',
              color: args.colors.area
            };
          } else if (args.showAllSymbol) {
            options.itemStyle.normal.lineStyle.width -= 1;
          }
          return angular.merge(args, options);
        },
        fillAxisData: function(options, data, inputs) {
          data = data || [];
          if (angular.isObject(inputs)) {
            if (angular.isString(inputs.groupId) && inputs.groupId.length) {
              options.groupId = inputs.groupId;
            }
            if (inputs.visibleDataPointsNum > 0) {
              options.visibleDataPointsNum = inputs.visibleDataPointsNum;
              var placeholder = {
                x: '',
                y: options.series.map(function() {
                  return {
                    value: '-',
                    tooltip: {}
                  };
                })
              };
              while (data.length < inputs.visibleDataPointsNum) {
                data.unshift(placeholder);
              }
            }
          }
          var dataSplit = splitInitialData(data, options.visibleDataPointsNum);
          if (dataSplit.newer.length) {
            options.dataPointsQueue = dataSplit.newer;
          }
          angular.forEach(options.series, function(series) {
            series.data = [];
          });
          if (options.xAxis[0].type === 'time') {
            angular.forEach(dataSplit.older, function(datum) {
              angular.forEach(options.series, function(series, seriesIndex) {
                series.data.push([datum.x, Array.isArray(datum.y) ? datum.y[seriesIndex] : datum.y]);
              });
            });
          } else {
            var xLabels = [];
            angular.forEach(dataSplit.older, function(datum) {
              xLabels.push(datum.x);
              angular.forEach(options.series, function(series, seriesIndex) {
                series.data.push(Array.isArray(datum.y) ? datum.y[seriesIndex] : datum.y);
              });
            });
            options.xAxis[0].data = xLabels;
          }
        },
        linkFn: function(scope, toEchartOptionFn) {
          scope.$watch('data', function(data) {
            if (data) {
              var dataArray = Array.isArray(data) ? data : [data];
              scope.api.addDataPoints(dataArray);
            }
          });
          scope.$watch('options', function(newOptions, oldOptions) {
            if (!angular.equals(newOptions, oldOptions)) {
              scope.api.rebuild(toEchartOptionFn(newOptions, scope));
            }
          }, true);
        }
      };
    }
  ]);
angular.module('dashing.charts.bar', [
  'dashing.charts.adapter.echarts',
  'dashing.charts.look_and_feel',
  'dashing.util'
])
  .directive('barChart', ['dashing.charts.look_and_feel', 'dashing.util', '$echarts',
    function(lookAndFeel, util, $echarts) {
      function toEchartOptions(dsOptions, scope) {
        var use = angular.merge({
          yAxisSplitNum: 3,
          yAxisShowMinorAxisLine: false,
          yAxisLabelWidth: 60,
          yAxisLabelFormatter: $echarts.axisLabelFormatter(''),
          static: true,
          rotate: false,
          xAxisShowLabels: true,
          margin: {
            left: undefined,
            right: undefined,
            top: undefined,
            bottom: undefined
          }
        }, dsOptions);
        use = angular.merge({
          barMaxWidth: use.rotate ? 20 : 16,
          barMaxSpacing: use.rotate ? 5 : 4,
          barMinWidth: use.rotate ? 6 : 4,
          barMinSpacing: use.rotate ? 2 : 1
        }, use);
        var data = use.data;
        if (!Array.isArray(data)) {
          console.warn({
            message: 'Initial data is expected to be an array',
            data: data
          });
          data = data ? [data] : [];
        }
        $echarts.validateSeriesNames(use, data);
        if (!Array.isArray(use.colors) || !use.colors.length) {
          use.colors = lookAndFeel.barChartColorRecommendation(use.seriesNames.length || 1);
        }
        var colors = use.colors.map(function(base) {
          return lookAndFeel.buildColorStates(base);
        });
        var axisColor = colors.length > 1 ? '#999' : colors[0].line;
        var minMargin = 15;
        var horizontalMargin = Math.max(minMargin, use.yAxisLabelWidth);
        var options = {
          height: use.height,
          width: use.width,
          tooltip: angular.merge(
            $echarts.categoryTooltip(use.valueFormatter), {
              axisPointer: {
                type: 'shadow',
                shadowStyle: {
                  color: 'rgba(225,225,225,0.3)'
                }
              }
            }),
          grid: angular.merge({
            borderWidth: 0,
            x: use.margin.left || horizontalMargin,
            x2: use.margin.right || horizontalMargin,
            y: use.margin.top || minMargin,
            y2: use.margin.bottom || minMargin + 13
          }, use.grid),
          xAxis: [{
            axisLabel: {
              show: true
            },
            axisLine: {
              show: true,
              lineStyle: {
                width: 1,
                color: axisColor,
                type: 'dotted'
              }
            },
            axisTick: false,
            splitLine: false
          }],
          yAxis: [{
            type: 'value',
            splitNumber: use.yAxisSplitNum,
            splitLine: {
              show: use.yAxisShowMinorAxisLine,
              lineStyle: {
                color: axisColor,
                type: 'dotted'
              }
            },
            axisLine: false,
            axisLabel: {
              formatter: use.yAxisLabelFormatter
            }
          }],
          series: use.seriesNames.map(function(name, i) {
            return $echarts.makeDataSeries({
              type: 'bar',
              name: name,
              stack: true,
              colors: colors[i]
            });
          }),
          color: use.colors
        };
        if (use.static) {
          delete use.visibleDataPointsNum;
        }
        $echarts.fillAxisData(options, data, use);
        if (use.static) {
          options.visibleDataPointsNum = -1;
        }
        if (use.rotate) {
          var axisSwap = options.xAxis;
          options.xAxis = angular.copy(options.yAxis);
          options.xAxis[0].type = options.xAxis[0].type || 'value';
          options.yAxis = axisSwap;
          options.yAxis[0].type = options.yAxis[0].type || 'category';
        }
        if (!use.xAxisShowLabels) {
          options.xAxis[0].axisLabel = false;
          options.grid.y2 = options.grid.y;
        }
        if (use.static) {
          var drawBarMinWidth = use.barMinWidth + use.barMinSpacing;
          var drawBarMaxWidth = use.barMaxWidth + use.barMaxSpacing;
          var drawAllBarMinWidth = data.length * drawBarMinWidth;
          var drawAllBarMaxWidth = data.length * drawBarMaxWidth;
          var chartHeight = parseInt(use.height);
          if (use.rotate) {
            var gridMarginY = options.grid.borderWidth * 2 + options.grid.y + options.grid.y2;
            if (chartHeight < gridMarginY + drawAllBarMinWidth) {
              console.info('The chart is too short to hold so many bars, so that we increase the height to ' +
                (gridMarginY + drawAllBarMinWidth) + 'px for you.');
              options.height = (gridMarginY + drawAllBarMinWidth) + 'px';
            } else if (chartHeight > gridMarginY + drawAllBarMaxWidth) {
              options.height = (gridMarginY + drawAllBarMaxWidth) + 'px';
            }
          } else {
            var gridMarginX = options.grid.borderWidth * 2 + options.grid.x + options.grid.x2;
            var chartControlWidth = scope.getChartControlWidthFn();
            var visibleWidthForBars = chartControlWidth - gridMarginX;
            if (drawAllBarMinWidth > 0 && drawAllBarMinWidth > visibleWidthForBars) {
              var roundedVisibleWidthForBars = Math.floor(visibleWidthForBars / drawBarMinWidth) * drawBarMinWidth;
              options.grid.x2 += visibleWidthForBars - roundedVisibleWidthForBars;
              var scrollbarHeight = 20;
              var scrollbarGridMargin = 5;
              options.dataZoom = {
                show: true,
                end: roundedVisibleWidthForBars * 100 / drawAllBarMinWidth,
                realtime: true,
                height: scrollbarHeight,
                y: chartHeight - scrollbarHeight - scrollbarGridMargin,
                handleColor: axisColor
              };
              options.dataZoom.fillerColor = util.color.alpha(options.dataZoom.handleColor, 0.08);
              options.grid.y2 += scrollbarHeight + scrollbarGridMargin * 2;
            } else if (data.length) {
              if (visibleWidthForBars > drawAllBarMaxWidth) {
                options.grid.x2 += chartControlWidth - drawAllBarMaxWidth - gridMarginX;
              } else if (!angular.isDefined(use.margin.right)) {
                roundedVisibleWidthForBars = Math.floor(visibleWidthForBars / data.length) * data.length;
                options.grid.x2 += visibleWidthForBars - roundedVisibleWidthForBars;
              }
            }
          }
        }
        return options;
      }
      return {
        restrict: 'E',
        template: '<echart options="::initOptions" api="api" on-resize="handleResize()"></echart>',
        scope: {
          options: '=optionsBind',
          data: '=datasourceBind'
        },
        link: function(scope) {
          return $echarts.linkFn(scope, toEchartOptions);
        },
        controller: ['$scope', '$element',
          function($scope, $element) {
            $scope.getChartControlWidthFn = function() {
              return angular.element($element[0]).children()[0].offsetWidth;
            };
            $scope.initOptions = toEchartOptions($scope.options, $scope);
            $scope.handleResize = function() {
              $scope.options._dirty = new Date();
              return true;
            };
          }
        ]
      };
    }
  ]);
angular.module('dashing.charts.line', [
  'dashing.charts.adapter.echarts',
  'dashing.charts.look_and_feel'
])
  .directive('lineChart', ['dashing.charts.look_and_feel', '$echarts',
    function(lookAndFeel, $echarts) {
      function toEchartOptions(dsOptions) {
        var use = angular.merge({
          seriesStacked: true,
          seriesLineSmooth: false,
          showLegend: true,
          yAxisSplitNum: 3,
          yAxisShowSplitLine: true,
          yAxisLabelWidth: 60,
          yAxisLabelFormatter: $echarts.axisLabelFormatter(''),
          yAxisScaled: false,
          xAxisShowLabels: true,
          margin: {
            left: undefined,
            right: undefined,
            top: undefined,
            bottom: undefined
          }
        }, dsOptions);
        var data = use.data;
        $echarts.validateSeriesNames(use, data);
        if (!Array.isArray(use.colors) || !use.colors.length) {
          use.colors = lookAndFeel.lineChartColorRecommendation(use.seriesNames.length || 1);
        }
        var colors = use.colors.map(function(base) {
          return lookAndFeel.buildColorStates(base);
        });
        var axisColor = '#ccc';
        var borderLineStyle = {
          length: 4,
          lineStyle: {
            width: 1,
            color: axisColor
          }
        };
        var horizontalMargin = Math.max(15, use.yAxisLabelWidth);
        var options = {
          height: use.height,
          width: use.width,
          tooltip: angular.merge(
            $echarts.categoryTooltip(use.valueFormatter), {
              axisPointer: {
                type: 'line',
                lineStyle: {
                  width: 3,
                  color: 'rgb(235,235,235)',
                  type: 'dotted'
                }
              }
            }),
          grid: angular.merge({
            borderWidth: 0,
            x: use.margin.left || horizontalMargin,
            x2: use.margin.right || horizontalMargin,
            y: use.margin.top || 20,
            y2: use.margin.bottom || 25
          }, use.grid),
          xAxis: [{
            type: use.xAxisTypeIsTime ? 'time' : undefined,
            boundaryGap: use.xAxisBoundaryGap,
            axisLine: angular.merge({
              onZero: false
            }, borderLineStyle),
            axisTick: borderLineStyle,
            axisLabel: {
              show: true
            },
            splitLine: false
          }],
          yAxis: [{
            splitNumber: use.yAxisSplitNum,
            splitLine: {
              show: use.yAxisShowSplitLine,
              lineStyle: {
                color: axisColor,
                type: 'dotted'
              }
            },
            axisLine: false,
            axisLabel: {
              formatter: use.yAxisLabelFormatter
            },
            scale: use.yAxisScaled
          }],
          series: use.seriesNames.map(function(name, i) {
            return $echarts.makeDataSeries({
              name: name,
              colors: colors[i],
              stack: use.seriesStacked,
              smooth: use.seriesLineSmooth,
              showAllSymbol: use.showAllSymbol,
              yAxisIndex: Array.isArray(use.seriesYAxisIndex) ?
                use.seriesYAxisIndex[i] : 0
            });
          }),
          color: use.colors
        };
        if (_.contains(use.seriesYAxisIndex, 1)) {
          var yAxis2 = angular.copy(options.yAxis[0]);
          if (angular.isFunction(use.yAxis2LabelFormatter)) {
            yAxis2.axisLabel.formatter = use.yAxis2LabelFormatter;
          }
          options.yAxis.push(yAxis2);
        }
        $echarts.fillAxisData(options, data, use);
        if (!use.xAxisShowLabels) {
          options.xAxis[0].axisLabel = false;
          options.grid.y2 = options.grid.y;
        }
        if (use.xAxisTypeIsTime) {
          $echarts.timelineChartFix(options, use);
        }
        if (options.series.length === 1) {
          options.yAxis.boundaryGap = [0, 0.1];
        }
        var titleHeight = 20;
        var legendHeight = 16;
        if (use.title) {
          options.title = {
            text: use.title,
            x: 0,
            y: 3
          };
          options.grid.y += titleHeight;
        }
        var addLegend = options.series.length > 1 && use.showLegend;
        if (addLegend) {
          options.legend = {
            show: true,
            itemWidth: 8,
            data: options.series.map(function(series) {
              return series.name;
            })
          };
          options.legend.y = 6;
          options.grid.y += 14;
          if (use.title) {
            options.legend.y += titleHeight;
            options.grid.y += legendHeight;
          }
        }
        if (addLegend || use.title) {
          options.grid.y += 12;
        }
        return options;
      }
      return {
        restrict: 'E',
        template: '<echart options="::initOptions" api="api"></echart>',
        scope: {
          options: '=optionsBind',
          data: '=datasourceBind'
        },
        link: function(scope) {
          return $echarts.linkFn(scope, toEchartOptions);
        },
        controller: ['$scope',
          function($scope) {
            $scope.initOptions = toEchartOptions($scope.options);
          }
        ]
      };
    }
  ]);
angular.module('dashing.charts.look_and_feel', [
  'dashing.util'
])
  .factory('dashing.charts.look_and_feel', ['dashing.util',
    function(util) {
      var self = {
        lineChartColorRecommendation: function(seriesNum) {
          var colors = util.color.palette;
          switch (seriesNum) {
            case 1:
              return [colors.blue];
            case 2:
              return [colors.blue, colors.blueishGreen];
            default:
              return util.array.repeatArray([
                colors.blue,
                colors.purple,
                colors.blueishGreen,
                colors.darkRed,
                colors.orange
              ], seriesNum);
          }
        },
        barChartColorRecommendation: function(seriesNum) {
          var colors = util.color.palette;
          switch (seriesNum) {
            case 1:
              return [colors.lightBlue];
            case 2:
              return [colors.blue, colors.darkBlue];
            default:
              return util.array.repeatArray([
                colors.lightGreen,
                colors.darkGray,
                colors.lightBlue,
                colors.blue,
                colors.darkBlue
              ], seriesNum);
          }
        },
        ringChartColorRecommendation: function(seriesNum) {
          return self.barChartColorRecommendation(seriesNum);
        },
        buildColorStates: function(base) {
          return {
            line: base,
            area: util.color.lighter(base, -0.92),
            hover: util.color.lighter(base, 0.15)
          };
        }
      };
      return self;
    }
  ]);
angular.module('dashing.charts.metrics_sparkline', [
  'dashing.charts.sparkline',
  'dashing.metrics'
])
  .directive('metricsSparklineChartTd', function() {
    return {
      restrict: 'E',
      templateUrl: 'app/dashing/charts/metrics_sparkline_td.html',
      scope: {
        caption: '@',
        help: '@',
        current: '@',
        unit: '@',
        subText: '@',
        options: '=optionsBind',
        data: '=datasourceBind'
      }
    };
  });
angular.module('dashing.charts.ring', [
  'dashing.charts.adapter.echarts',
  'dashing.charts.look_and_feel'
])
  .directive('ringChart', function() {
    return {
      restrict: 'E',
      template: '<echart options="::initOptions" api="api"></echart>',
      scope: {
        options: '=optionsBind',
        data: '=datasourceBind'
      },
      link: function(scope) {
        scope.$watch('data', function(data) {
          scope.api.updateOption({
            series: [{
              data: [{
                value: data.available.value
              }, {
                value: data.used.value
              }]
            }]
          });
        });
      },
      controller: ['$scope', '$element', 'dashing.charts.look_and_feel',
        function($scope, $element, lookAndFeel) {
          var use = angular.merge({
            color: lookAndFeel.ringChartColorRecommendation(1)[0],
            thickness: 0.25
          }, $scope.options);
          if (!angular.isNumber(use.thickness) || use.thickness > 1 || use.thickness <= 0) {
            console.warn({
              message: 'Ignored invalid thickness value',
              value: use.thickness
            });
            use.thickness = 0.25;
          }
          var data = use.data || $scope.data;
          if (!data) {
            console.warn('Need data to render the ring chart.');
          }
          var colors = lookAndFeel.buildColorStates(use.color);
          var padding = 8;
          var outerRadius = (parseInt(use.height) - 30 - padding * 2) / 2;
          var innerRadius = Math.floor(outerRadius * (1 - use.thickness));
          var innerTextFontSize = Math.floor(28 * innerRadius / 39);
          if (innerTextFontSize < 12) {
            console.warn('Please increase the height to get a better visual experience.');
          }
          var itemStyleBase = {
            normal: {
              color: 'rgb(232,239,240)',
              label: {
                show: true,
                position: 'center'
              },
              labelLine: false
            }
          };
          var options = {
            height: use.height,
            width: use.width,
            grid: {
              borderWidth: 0
            },
            xAxis: [{
              show: false,
              data: [0]
            }],
            legend: {
              selectedMode: false,
              itemGap: 20,
              itemWidth: 13,
              y: 'bottom',
              data: [data.used.label, data.available.label].map(function(label) {
                return {
                  name: label,
                  textStyle: {
                    fontWeight: 500
                  },
                  icon: 'a'
                };
              })
            },
            series: [{
              type: 'pie',
              center: ['50%', outerRadius + padding],
              radius: [innerRadius, outerRadius],
              data: [{
                name: data.available.label,
                value: data.available.value,
                itemStyle: itemStyleBase
              }, {
                name: data.used.label,
                value: data.used.value,
                itemStyle: angular.merge({}, itemStyleBase, {
                  normal: {
                    color: colors.line
                  }
                })
              }]
            }]
          };
          options.series[0].itemStyle = {
            normal: {
              label: {
                formatter: function() {
                  return Math.round($scope.data.used.value * 100 /
                    ($scope.data.used.value + $scope.data.available.value)) + '%';
                },
                textStyle: {
                  color: '#111',
                  fontSize: Math.floor(28 * innerRadius / 39),
                  fontWeight: 500,
                  baseline: 'middle'
                }
              }
            }
          };
          if (use.title) {
            options.series[0].center[0] = outerRadius + padding;
            options.legend.x = padding;
            options.title = {
              text: use.title,
              x: (outerRadius + padding) * 2 + padding + 4,
              y: outerRadius + padding + 4,
              textStyle: {
                fontSize: 12,
                fontWeight: 500,
                color: '#666'
              }
            };
            var left = options.title.x + 14;
            var top = options.title.y - 48;
            var total = $scope.data.used.value + $scope.data.available.value;
            var unit = $scope.data.used.unit;
            var unselectable =
              '-webkit-touch-callout: none;' +
              '-webkit-user-select: none;' +
              '-khtml-user-select: none;' +
              '-moz-user-select: none;' +
              '-ms-user-select: none;' +
              'user-select: none;';
            angular.element($element[0]).append([
              '<div style="position: absolute; left: ' + left + 'px; top: ' + top + 'px">',
              '<p style="cursor: default; ' + unselectable + '">',
              '<span style="font-size: 40px; font-weight: 500">' + total + '</span>', (unit ? ('<span style="font-size: 15px; font-weight: 700">' + unit + '</span>') : ''),
              '</p>',
              '</div>'
            ].join(' '));
          }
          $scope.initOptions = options;
        }
      ]
    };
  });
angular.module('dashing.charts.sparkline', [
  'dashing.charts.adapter.echarts',
  'dashing.charts.look_and_feel'
])
  .directive('sparklineChart', ['dashing.charts.look_and_feel', '$echarts',
    function(lookAndFeel, $echarts) {
      function toEchartOptions(dsOptions) {
        var use = angular.merge({
          color: lookAndFeel.lineChartColorRecommendation(1)[0],
          yAxisBoundaryGap: [0, 0.5]
        }, dsOptions);
        if (use.xAxisTypeIsTime) {
          console.warn('Echarts does not have a good experience for time series, so we fallback to category. ' +
            'Please track https://github.com/ecomfe/echarts/issues/1954');
          use.xAxisTypeIsTime = false;
        }
        var colors = lookAndFeel.buildColorStates(use.color);
        var defaultMargin = 5;
        var options = {
          height: use.height,
          width: use.width,
          tooltip: $echarts.categoryTooltip(use.valueFormatter),
          grid: angular.merge({
            borderWidth: 1,
            x: defaultMargin,
            y: defaultMargin,
            x2: defaultMargin,
            y2: 1
          }, use.grid),
          xAxis: [{
            type: use.xAxisTypeIsTime ? 'time' : undefined,
            boundaryGap: false,
            axisLine: false,
            axisLabel: false,
            splitLine: false
          }],
          yAxis: [{
            boundaryGap: use.yAxisBoundaryGap,
            show: false
          }],
          series: [$echarts.makeDataSeries({
            colors: colors,
            stack: true
          })]
        };
        if (use.series0Type === 'bar') {
          options.grid.borderWidth = 0;
          options.grid.y2 = 0;
          options.xAxis[0].boundaryGap = true;
          options.series[0].type = 'bar';
        }
        var data = use.data;
        $echarts.fillAxisData(options, data, use);
        return options;
      }
      return {
        restrict: 'E',
        template: '<echart options="::initOptions" api="api"></echart>',
        scope: {
          options: '=optionsBind',
          data: '=datasourceBind'
        },
        link: function(scope) {
          return $echarts.linkFn(scope, toEchartOptions);
        },
        controller: ['$scope',
          function($scope) {
            $scope.initOptions = toEchartOptions($scope.options);
          }
        ]
      };
    }
  ]);
angular.module('dashing.contextmenu', [
  'mgcrea.ngStrap.dropdown'
])
  .factory('$contextmenu', function() {
    return {
      popup: function(elem, position) {
        var elem0 = angular.element(elem);
        elem0.css({
          left: position.x + 'px',
          top: position.y + 'px'
        });
        elem0.triggerHandler('click');
      }
    };
  });
angular.module('dashing.dialogs', [
  'mgcrea.ngStrap.modal'
])
  .factory('$dialogs', ['$modal', 'dashing.i18n',
    function($modal, i18n) {
      function createModalDialog(options, onClose) {
        var modalCloseEventName = 'modal.onclose';
        var dialog = $modal(angular.merge({
          show: true,
          backdrop: 'static',
          controller: ['$scope',
            function($scope) {
              $scope.text = options.text;
              var plainContent = options.content.replace(/<[^>]+>/gm, '');
              $scope.size = plainContent.length <= 60 ? 'modal-sm' : '';
              $scope.close = function(modalValue) {
                $scope.$emit(modalCloseEventName, {
                  modalValue: modalValue
                });
                $scope.$hide();
              }
            }
          ]
        }, options));
        if (angular.isFunction(onClose)) {
          dialog.$scope.$on(modalCloseEventName, function(_, values) {
            onClose(values.modalValue);
          });
        }
        return dialog;
      }
      return {
        confirm: function(text, onConfirm) {
          var options = {
            templateUrl: 'app/dashing/dialogs/confirmation.html',
            title: i18n.confirmationDialogTitle,
            text: {
              yesButton: i18n.confirmationYesButtonText,
              noButton: i18n.confirmationNoButtonText
            },
            content: text
          };
          var handleCloseFn = function(modalValue) {
            if (modalValue > 0) {
              onConfirm();
            }
          };
          createModalDialog(options, handleCloseFn);
        },
        notice: function(text, title) {
          var options = {
            templateUrl: 'app/dashing/dialogs/notification.html',
            title: title || i18n.notificationDialogTitle,
            text: {
              closeButton: i18n.closeButtonText
            },
            content: text
          };
          createModalDialog(options);
        }
      };
    }
  ]);
angular.module('dashing.filters.any', [])
  .filter('any', function() {
    return function(items, props) {
      if (!Array.isArray(items)) {
        return items;
      }
      return items.filter(function(item) {
        var keys = Object.keys(props);
        for (var i = 0; i < keys.length; i++) {
          var prop = keys[i];
          var subtext = angular.lowercase(props[prop] || '');
          var text = angular.lowercase(item[prop] || '');
          if (text.indexOf(subtext) !== -1) {
            return true;
          }
        }
        return false;
      });
    }
  });
angular.module('dashing.filters.duration', [
  'dashing.util'
])
  .filter('duration', ['dashing.util',
    function(util) {
      return function(millis, compact) {
        return util.text.toHumanReadableDuration(millis, compact);
      };
    }
  ]);
angular.module('dashing.forms.form_control', [
  'ngSanitize',
  'dashing.filters.any',
  'dashing.util.validation',
  'mgcrea.ngStrap',
  'ui.select'
])
  .directive('formControl', ['dashing.util.validation',
    function(validation) {
      function buildChoicesForSelect(choices) {
        var result = [];
        angular.forEach(choices, function(choice, value) {
          var item = {
            value: value
          };
          if (angular.isString(choice)) {
            item.text = choice;
          } else {
            item.text = choice.text;
            if (choice.hasOwnProperty('subtext')) {
              item.subtext = choice.subtext;
            }
          }
          result.push(item);
        });
        return result;
      }
      function buildChoicesForRadioGroup(choices) {
        var result = [];
        angular.forEach(choices, function(choice, value) {
          result.push({
            value: value,
            text: choice
          });
        });
        return result;
      }
      function buildChoicesForDropDownMenu(choices, onSelect) {
        return choices.map(function(choice) {
          if (angular.isString(choice)) {
            choice = {
              text: choice
            };
          }
          return {
            text: (choice.icon ? '<i class="' + choice.icon + '"></i> ' : '') + choice.text,
            click: function() {
              onSelect(choice.text);
            }
          };
        });
      }
      function fixDateTimeControlMarginResponsive(scope) {
        var widthBreakPoint = 768;
        scope.$watch(function() {
          return window.innerWidth;
        }, function(width) {
          scope.timeControlLeftMargin = width < widthBreakPoint ? '15px' : '0';
          scope.timeControlTopMargin = width < widthBreakPoint ? '4px' : '0';
        });
        window.onresize = function() {
          scope.$apply();
        };
      }
      return {
        restrict: 'E',
        templateUrl: 'app/dashing/forms/form_controls.html',
        replace: true,
        scope: {
          help: '@',
          value: '=ngModel',
          invalid: '=?'
        },
        link: function(scope, elem, attrs) {
          scope.labelStyleClass = attrs.labelStyleClass || 'col-sm-3';
          scope.controlStyleClass = attrs.controlStyleClass || 'col-sm-9';
          scope.choiceIconStyleClass = attrs.choiceIconStyleClass || 'glyphicon glyphicon-menu-hamburger';
          scope.label = attrs.label;
          scope.renderAs = attrs.type;
          scope.pristine = true;
          scope.invalid = attrs.required;
          switch (attrs.type) {
            case 'class':
              scope.renderAs = 'text';
              scope.validateFn = validation.class;
              break;
            case 'choices':
              scope.placeholder = attrs.searchPlaceholder;
              scope.choices = buildChoicesForSelect(eval('(' + attrs.choices + ')'));
              scope.allowSearchInChoices = attrs.hasOwnProperty('searchEnabled') ?
                (attrs.searchEnabled === 'true') : Object.keys(scope.choices).length >= 5;
              scope.allowClearSelection = !attrs.required;
              break;
            case 'radio':
              scope.choices = buildChoicesForRadioGroup(eval('(' + attrs.choices + ')'));
              scope.buttonStyleClass = attrs.btnStyleClass || 'btn-sm';
              scope.toggle = function(value) {
                scope.value = value;
              };
              break;
            case 'multi-checks':
              scope.choices = eval('(' + attrs.choices + ')');
              if (!Array.isArray(scope.choices)) {
                scope.choices = [attrs.choices];
              }
              if (!Array.isArray(scope.value)) {
                scope.value = scope.choices.map(function() {
                  return false;
                });
              }
              break;
            case 'check':
              scope.text = scope.label;
              scope.label = '';
              break;
            case 'integer':
              scope.min = attrs.min;
              scope.max = attrs.max;
              scope.validateFn = function(value) {
                return validation.integerInRange(value, attrs.min, attrs.max);
              };
              break;
            case 'datetime':
              scope.dateControlStyleClass = attrs.dateControlStyleClass || 'col-sm-5';
              scope.timeControlStyleClass = attrs.timeControlStyleClass || 'col-sm-4';
              fixDateTimeControlMarginResponsive(scope);
              scope.fillDefaultDate = function() {
                if (!scope.dateValue) {
                  scope.dateValue = new Date();
                }
              };
              scope.fillDefaultTime = function() {
                if (!scope.timeValue) {
                  var now = new Date();
                  now.setSeconds(0);
                  now.setMilliseconds(0);
                  scope.timeValue = now;
                }
              };
              scope.dateInputInvalid = false;
              scope.timeInputInvalid = false;
              scope.$watch('dateValue', function(newVal, oldVal) {
                scope.dateInputInvalid = angular.isUndefined(newVal) && !angular.isUndefined(oldVal);
                scope.invalid = scope.dateInputInvalid || scope.timeInputInvalid;
                if (newVal) {
                  scope.value = [newVal, scope.timeValue];
                }
              });
              scope.$watch('timeValue', function(newVal, oldVal) {
                scope.timeInputInvalid = angular.isUndefined(newVal) && !angular.isUndefined(oldVal);
                scope.invalid = scope.dateInputInvalid || scope.timeInputInvalid;
                if (newVal) {
                  scope.value = [scope.dateValue, newVal];
                }
              });
              scope.$watchCollection('value', function(val) {
                if (Array.isArray(val) && val.length === 2) {
                  scope.dateValue = val[0];
                  scope.timeValue = val[1];
                }
              });
              break;
            case 'upload':
              scope.acceptPattern = attrs.acceptPattern;
              scope.filename = '';
              scope.$watch('files', function(files) {
                if (Array.isArray(files) && files.length) {
                  scope.value = files[0];
                  scope.filename = files[0].name;
                }
              });
              scope.openUpload = function() {
                var spans = elem.find('span');
                if (spans.length > 2) {
                  var uploadButton = spans[spans.length - 2];
                  uploadButton.click();
                }
              };
              scope.clearSelection = function() {
                scope.value = null;
                scope.filename = '';
              };
              break;
            default:
              scope.hideMenuCaret = ['true', '1'].indexOf(String(attrs.hideMenuCaret)) !== -1;
              break;
          }
          if (scope.renderAs === 'text' && attrs.choices) {
            scope.choicesMenu = buildChoicesForDropDownMenu(
              eval('(' + attrs.choices + ')'),
              function(choice) {
                scope.value = choice;
              });
          }
          scope.$watch('value', function(value) {
            scope.pristine = (attrs.type !== 'integer') && (value || '').length === 0;
            scope.invalid =
              (angular.isFunction(scope.validateFn) && !scope.validateFn(value)) ||
              (attrs.required && scope.pristine);
          });
        }
      };
    }
  ]);
angular.module('dashing.forms.searchbox', [])
  .directive('searchbox', function() {
    return {
      restrict: 'E',
      templateUrl: 'app/dashing/forms/searchbox.html',
      scope: {
        placeholder: '@',
        ngModel: '='
      },
      controller: ['$scope',
        function($scope) {
          $scope.hint = $scope.placeholder;
          $scope.hideHint = function() {
            $scope.hint = '';
          };
          $scope.restoreHint = function() {
            if (!$scope.ngModel) {
              $scope.hint = $scope.placeholder;
            }
          };
        }
      ]
    };
  });
angular.module('dashing')
  .constant('dashing.i18n', {
    emptySearchResult: 'No results matched your search :-(',
    paginationSummary: 'Showing {{ stRange.from }}-{{ stRange.to }} of {{ totalItemCount }} records',
    confirmationDialogTitle: 'Confirmation',
    confirmationYesButtonText: 'Yes',
    confirmationNoButtonText: 'No, Thanks',
    notificationDialogTitle: 'Notification',
    closeButtonText: 'Close'
  });
angular.module('dashing.metrics', [])
  .directive('metrics', function() {
    return {
      restrict: 'E',
      templateUrl: 'app/dashing/metrics/metrics.html',
      scope: {
        caption: '@',
        value: '@',
        unit: '@',
        unitPlural: '@',
        subText: '@',
        help: '@',
        remarkType: '@',
        clickHelp: '&'
      }
    };
  });
angular.module('dashing.progressbar', [])
  .directive('progressbar', function() {
    return {
      restrict: 'E',
      templateUrl: 'app/dashing/progressbar/progressbar.html',
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
angular.module('dashing.property.number', [
  'dashing.util'
])
  .directive('number', ['$filter', 'dashing.util',
    function($filter, util) {
      return {
        restrict: 'E',
        templateUrl: 'app/dashing/property/number.html',
        scope: {
          raw: '@'
        },
        link: function(scope, elem, attrs) {
          var readable = ['true', '1'].indexOf(String(attrs.readable)) !== -1;
          attrs.$observe('raw', function(raw) {
            if (readable) {
              var hr = util.text.toHumanReadableNumber(Number(raw), 1024);
              scope.value = hr.value.toFixed(0);
              scope.unit = hr.modifier + attrs.unit;
            } else {
              scope.value = $filter('number')(raw, Number(attrs.precision) || 0);
              scope.unit = attrs.unit;
            }
          });
        }
      };
    }
  ]);
angular.module('dashing.property', [
  'mgcrea.ngStrap.tooltip'
])
  .directive('property', function() {
    return {
      restrict: 'E',
      templateUrl: 'app/dashing/property/property.html',
      replace: false,
      scope: {
        value: '=valueBind',
        renderer: '@'
      },
      controller: ['$scope', 'dsPropertyRenderer',
        function($scope, renderer) {
          if ($scope.renderer === renderer.BYTES) {
            console.warn('deprecated: should use renderer NUMBER instead');
            $scope.renderer = renderer.NUMBER;
          }
          $scope.$watch('value', function(value) {
            if (angular.isObject(value)) {
              switch ($scope.renderer) {
                case renderer.LINK:
                  if (!value.href) {
                    $scope.href = value.text;
                  }
                  break;
                case renderer.BUTTON:
                  if (value.href && !value.click) {
                    $scope.click = function() {
                      if (value.target) {
                        var win = window.open(value.href, value.target);
                        win.focus();
                      } else {
                        location.href = value.href;
                      }
                    };
                  }
                  break;
              }
              if (value.hasOwnProperty('value')) {
                console.warn({
                  message: 'Ignore `value.value`, because it is a reversed field.',
                  object: value
                });
                delete value.value;
              }
              angular.merge($scope, value);
            } else if (angular.isNumber(value)) {
              if ($scope.renderer === renderer.NUMBER) {
                $scope.raw = value;
              }
            }
          });
        }
      ]
    };
  })
  .constant('dsPropertyRenderer', {
    BUTTON: 'Button',
    BYTES: 'Bytes',
    DATETIME: 'DateTime',
    DURATION: 'Duration',
    INDICATOR: 'Indicator',
    LINK: 'Link',
    NUMBER: 'Number',
    NUMBER1: 'Number1',
    NUMBER2: 'Number2',
    PROGRESS_BAR: 'ProgressBar',
    TAG: 'Tag',
    TEXT: undefined
  });
angular.module('dashing.remark', [
  'mgcrea.ngStrap.tooltip'
])
  .directive('remark', function() {
    return {
      restrict: 'E',
      templateUrl: 'app/dashing/remark/remark.html',
      scope: {
        tooltip: '@',
        placement: '@',
        type: '@',
        trigger: '@'
      },
      link: function(scope) {
        scope.$watch('type', function(type) {
          switch (type) {
            case 'info':
              scope.styleClass = 'glyphicon glyphicon-info-sign remark-icon';
              break;
            case 'warning':
              scope.styleClass = 'glyphicon glyphicon-warning-sign remark-icon-warning';
              break;
            default:
              scope.styleClass = 'glyphicon glyphicon-question-sign remark-icon';
              break;
          }
        });
      }
    };
  });
angular.module('dashing.state.indicator', [
  'dashing.util',
  'mgcrea.ngStrap.tooltip'
])
  .directive('indicator', ['dashing.util',
    function(util) {
      return {
        restrict: 'E',
        templateUrl: 'app/dashing/state/indicator.html',
        scope: {
          tooltip: '@',
          shape: '@'
        },
        link: function(scope, elem, attrs) {
          if (!attrs.condition) {
            attrs.condition = '';
          }
          attrs.$observe('condition', function(condition) {
            scope.colorStyle = util.color.conditionToColor(condition);
          });
          attrs.$observe('tooltip', function(tooltip) {
            scope.cursorStyle = tooltip ? 'pointer' : 'default';
          });
        }
      };
    }
  ]);
angular.module('dashing.state.tag', [
  'dashing.util',
  'mgcrea.ngStrap.tooltip'
])
  .directive('tag', ['dashing.util',
    function(util) {
      return {
        restrict: 'E',
        templateUrl: 'app/dashing/state/tag.html',
        scope: {
          href: '@',
          text: '@',
          tooltip: '@'
        },
        link: function(scope, elem, attrs) {
          if (!attrs.condition) {
            attrs.condition = '';
          }
          attrs.$observe('condition', function(condition) {
            scope.labelColorClass = util.bootstrap.conditionToBootstrapLabelClass(condition);
          });
          attrs.$observe('tooltip', function(tooltip) {
            if (!scope.href) {
              scope.cursorStyle = tooltip ? 'pointer' : 'default';
            }
          });
        }
      };
    }
  ]);
angular.module('dashing.tables.property_table', [])
  .directive('propertyTable', function() {
    return {
      restrict: 'E',
      templateUrl: 'app/dashing/tables/property_table/property_table.html',
      scope: {
        caption: '@',
        captionTooltip: '@',
        subCaption: '@',
        props: '=propsBind',
        propNameClass: '@',
        propValueClass: '@'
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
        templateUrl: 'app/dashing/tables/sortable_table/sortable_table.html',
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
angular.module('dashing.tabset', [])
  .directive('tabset', [
    function() {
      return {
        restrict: 'E',
        templateUrl: 'app/dashing/tabset/tabset.html',
        transclude: true,
        scope: true,
        controller: ['$scope',
          function($scope) {
            var tabs = $scope.tabs = [];
            function select(tab, reload) {
              angular.forEach(tabs, function(item) {
                item.selected = item === tab;
              });
              if (tab.load !== undefined) {
                tab.load(reload);
              }
            }
            this.addTab = function(tab) {
              tabs.push(tab);
              if (tabs.length === 1) {
                select(tab);
              }
            };
            $scope.selectTab = function(activeTabIndex, reload) {
              if (activeTabIndex >= 0 && activeTabIndex < tabs.length) {
                select(tabs[activeTabIndex], reload);
              }
            };
          }
        ]
      };
    }
  ])
.directive('tab', ['$http', '$controller', '$compile',
  function($http, $controller, $compile) {
    return {
      restrict: 'E',
      require: '^tabset',
      template: '<div class="tab-pane" ng-class="{active:selected}" ng-transclude></div>',
      replace: true,
      transclude: true,
      link: function(scope, elem, attrs, ctrl) {
        scope.heading = attrs.heading;
        scope.loaded = false;
        scope.load = function(reload) {
          if (scope.loaded && !reload) {
            return;
          }
          if (attrs.template) {
            $http.get(attrs.template).then(function(response) {
              createTemplateScope(response.data);
            });
          }
        };
        function createTemplateScope(template) {
          elem.html(template);
          var templateScope = scope.$new(false);
          if (attrs.controller) {
            var scopeController = $controller(attrs.controller, {
              $scope: templateScope
            });
            elem.children().data('$ngController', scopeController);
          }
          $compile(elem.contents())(templateScope);
          scope.loaded = true;
        }
        ctrl.addTab(scope);
      }
    };
  }
]);
angular.module('dashing.util.array', [])
.factory('dashing.util.array', function() {
  return {
    alignArray: function(array, length, default_) {
      if (length <= array.length) {
        return array.slice(0, length);
      }
      var result = angular.copy(array);
      for (var i = result.length; i < length; i++) {
        result.push(default_);
      }
      return result;
    },
    repeatArray: function(array, sum) {
      if (sum <= array.length) {
        return array.slice(0, sum);
      }
      var result = [];
      for (var i = 0; i < sum; i++) {
        result.push(array[i % array.length]);
      }
      return result;
    },
    ensureArray: function(value) {
      return Array.isArray(value) ? value : [value];
    }
  };
});
angular.module('dashing.util.color', [])
.factory('dashing.util.color', function() {
  return {
    palette: {
      blue: 'rgb(0,119,215)',
      blueishGreen: 'rgb(41,189,181)',
      orange: 'rgb(255,127,80)',
      purple: 'rgb(110,119,215)',
      skyBlue: 'rgb(91,204,246)',
      darkBlue: 'rgb(102,168,212)',
      darkGray: 'rgb(92,92,97)',
      darkPink: 'rgb(212,102,138)',
      darkRed: 'rgb(212,102,138)',
      lightBlue: 'rgb(35,183,229)',
      lightGreen: 'rgb(169,255,150)'
    },
    conditionToColor: function(condition) {
      switch (condition) {
        case 'good':
          return '#5cb85c';
        case 'concern':
          return '#f0ad4e';
        case 'danger':
          return '#d9534f';
        default:
          return '#aaa';
      }
    },
    lighter: function(color, level) {
      return zrender.tool.color.lift(color, level);
    },
    alpha: function(color, transparency) {
      return zrender.tool.color.alpha(color, transparency);
    }
  };
});
angular.module('dashing.util.text', [])
.factory('dashing.util.text', function() {
  return {
    toHumanReadableNumber: function(value, base, digits) {
      var modifier = '';
      if (value !== 0) {
        if (base !== 1024) {
          base = 1000;
        }
        var positive = value > 0;
        var positiveValue = Math.abs(value);
        var s = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
        var e = Math.floor(Math.log(positiveValue) / Math.log(base));
        value = positiveValue / Math.pow(base, e);
        if (digits > 0 && value !== Math.floor(value)) {
          value = value.toFixed(digits);
        }
        if (!positive) {
          value *= -1;
        }
        modifier = s[e];
      }
      return {
        value: value,
        modifier: modifier
      };
    },
    toHumanReadableDuration: function(millis, compact) {
      var x = parseInt(millis, 10);
      if (isNaN(x)) {
        return millis;
      }
      var units = [{
        label: ' ms',
        mod: 1000
      }, {
        label: compact ? 's' : ' secs',
        mod: 60
      }, {
        label: compact ? 'm' : ' mins',
        mod: 60
      }, {
        label: compact ? 'h' : ' hours',
        mod: 24
      }, {
        label: compact ? 'd' : ' days',
        mod: 7
      }, {
        label: compact ? 'w' : ' weeks',
        mod: 52
      }];
      var duration = [];
      for (var i = 0; i < units.length; i++) {
        var unit = units[i];
        var t = x % unit.mod;
        if (t !== 0) {
          duration.unshift({
            label: unit.label,
            value: t
          });
        }
        x = (x - t) / unit.mod;
      }
      duration = duration.slice(0, 2);
      if (duration.length > 1 && duration[1].label === ' ms') {
        duration = [duration[0]];
      }
      return duration.map(function(unit) {
        return unit.value + unit.label;
      }).join(compact ? ' ' : ' and ');
    }
  };
});
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
