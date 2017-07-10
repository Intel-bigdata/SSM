# HISTORY
https://github.com/stanleyxu2005/dashing

The file only contains the major change history. For full change history, please check the commit log.

## 0.4.8
* Improvement (#65): Add a notification dialog service.

## 0.4.7
* Improvement (#63): Hide placeholder text when search box is focused.
* Improvement (#64): When file picker has a selection, click on the input box should show the file browse dialog again.

## 0.4.6
* Feature (#62): Create a confirmation dialog service.
* Improvement (#60): Hide table header when no records in table (not in search mode).
* Improvement (#61): Sort data point values in chart's tooltip by name.

## 0.4.5
* Bugfix (#57): Date picker was not rendered correctly for Firefox.
* Bugfix (#55): Date time picker control was not aligned on small screen.
* Bugfix (#59): Library breaks with angular 1.4.9.
* Updated example build script (angular 1.4.9 has conflict with attribute required="true").

## 0.4.4
* Improvment (#54): Tooltip of table caption will be right to the caption.

## 0.4.3
* Improvment (#54): Allow table caption to have tooltip and sub-caption.

## 0.4.2
* Improvment (#44): Allow to override line and bar chart's margin.
* Bugfix (#1): When window is resized, the bar chart should recalculate all bar positions.

## 0.4.1
* Feature (#51): Exposed charts apis to directive. Chart will be repainted, if option is changed.
* Improvement (#52): Optional to hide "0" on y-axis for line and bar charts.

## 0.4.0
* Improvement (#49): Auto select the current date for date picker.
* Improvement (#50): Allow to customize empty sortable table.

## 0.3.9
* Improvement (#2): Click sort arrow will show animation.
* Improvement: Able to align a numberic column to left by setting `.textLeft()`.
* Improvement: Tooltip y-position will be 10px below chart top border.
* Bugfix: Bar chart will have an integral bar width and updated the demo page.

## 0.3.8
* Improvement (#47): Use a more elegant/active color theme for bar chart.
* Bugfix (#46): Sortable table wont render records correctly if sort key's value equals 0.
* Bugfix (#45): Chart's right margin was too large (when only one y-axis is used).
* Bugfix (#42): Added a helper method to update `<sortable-table>` records and changed the pagination control.
* Bugfix (#41): `<sortable-table>` flicker, if default sort direction is descending.

## 0.3.7
* Feature (#22): Polished datetime picker.
* Improvement (#38): Add onclick event to remark icon of `<metrics>`.
* Improvement: Provide a bugfix style for stripe cell in `<sortable-table>`.

## 0.3.6
* Improvement (#26): Support file upload control as `<form-control>`.
* Improvement: `<form-control>` The ng-model of datetime picker is now an array with date string and time string.

## 0.3.5
* Improvement (#37): Pluralize `<metrics>` unit.
* Bugfix (#36): Remark icon might disappear.

## 0.3.4
* Improvement (#35): Open link in new page for property `<button>`.
* Bugfix (#10): Property `<indicator shape="stripe">` wont show in MSIE and Firefox.

## 0.3.3
* Improvement (#34): Allow `<metrics>` to show warning or information remark icon.

## 0.3.1
* Improvement (#33): Property `<bytes>` is deprecated. Use `<number>` instead, which provides precision, unit and human-readable.

## 0.3.0
* Improvement (#31): Allow Number1 and Number2 as showing 1 or 2 digits after period in property table.

## 0.2.9
* Breaking changes (#27): `<form-control>` numeric types are all called "integer". Specify the `min` to `0`, `1` or undefined for different validators. 
* Improvement (#27): `<form-control>` supports "multi-checks" and "check".
* Improvement (#29): `<property renderer="Link">` supports "target=_blank".
* Bugfix (#25): ui-select required style sheets were missing.
* Bugfix (#30): `$sortableTableBuilder` should warn developer to specify a sort key, if multipe keys are defined for a column.

## 0.2.8 (0.2.7 was published to npm corrupted)

## 0.2.7
* Improvement: Added two chinese fonts "Hiragino Sans GB" and "Microsoft YaHei" in style sheets.
* Improvement (#20): Fixed several look and feel glitches when charts are grouped together.
* Bugfix (#21): `<form-control>`'s label should not right aligned when page is very narrow.
* Bugfix: Always show axis line and ticks at the bottom of `<line-chart>`.

## 0.2.6
* Feature (#19): `<form-control>` support radio group.
* Feature (#18): `<line-chart>` allows to have a secondary y-axis.

## 0.2.5
* Feature (#13): Support to render 2-value data as a `<ring-chart>`.
* Improvement: Specify tooltip position for `<remark>`.
* Feature: Added `<form-control>` to build a consistent input form. Supports text/dropdown/numeric inputs with validation.

## 0.2.4
* Improvement (#12): Prior to 0.2.2, found a solution to hide tooltip when chart's selection is empty.

## 0.2.3 (Hotfix to 0.2.2)
* Bugfix : gulp would strip too many blank spaces in 0.2.2. 
* Improvement: Exposed method `toHumanReadableDuration` to util module.

## 0.2.2
* Improvement: If visibleDataPointsNum is specified, the chart will be initialized the empty point positions with placeholders. This change fixes animation flicker while adding data points.
* Feature (#6): Allows to group `<line-chart>` components. (Dashing-deps 0.0.7 is required)
* Improvement (#11): PropertyTableBuilder is able to change layout direction for a multiple renderers cell.

## 0.2.0
* Feature (#8): Polished data points of a `<line-chart>` by having a semi-transparent border.
* Feature (#4, #5): `<bar-chart>` has more layouts (including rotated chart, bar with negative color).

## 0.1.9
* Improvement: Thousand separator support for chart labels and tooltips.
* Improvement: npm will include release files and github will be responsible for source code only.

## 0.1.8
* Feature: Add timeline support for `<line-chart>`. Previously x-axis can only have string labels.

## 0.1.7
* Improvement: If no initial data is provided, the chart is still able to be created. With a "No Graph Data Available" animation.
* Bugfix: The default sort ordering of `<sortable-table>` was wrong.

## 0.1.6
* Feature: Auto format y-axis. By default it will be formatted in a human readable notation.
* Feature: `<line-chart>` will have minor split lines.

## 0.1.4
* Feature: Provided builder to build and update `<property-table>` and `<sortable-table>`.
* Improvement: `<bar-chart>` will calculate bar width regarding specified data values.
* Improvement: Moved the custom Echarts build to dashing-deps project.

*The older versions are not documented well. We start the change log from here.*
