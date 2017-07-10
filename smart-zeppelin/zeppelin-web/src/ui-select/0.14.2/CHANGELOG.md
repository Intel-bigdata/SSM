<a name="0.14.2"></a>
## [0.14.2](https://github.com/angular-ui/ui-select/compare/v0.14.1...v0.14.2) (2016-01-28)




<a name="0.14.1"></a>
## [0.14.1](https://github.com/angular-ui/ui-select/compare/v0.14.1...v0.14.1) (2016-01-27)




<a name="0.14.0"></a>
# [0.14.0](https://github.com/angular-ui/ui-select/compare/v0.13.3...v0.14.0) (2016-01-25)


### Features

* **ngAnimate:** add support for ngAnimate ([8da8a6d](https://github.com/angular-ui/ui-select/commit/8da8a6d))



<a name="0.13.3"></a>
## [0.13.3](https://github.com/angular-ui/ui-select/compare/v0.13.3...v0.13.2) (2016-01-25)

### Added
- Add support for commonjs and npm

<a name="0.13.2"></a>
## [0.13.2](https://github.com/angular-ui/ui-select/compare/v0.13.2...v0.13.2) (2016-01-25)


### Bug Fixes

* **CSP:** avoid inline execution of javascript in choices template. ([fb88ec8](https://github.com/angular-ui/ui-select/commit/fb88ec8))



# Change Log
All notable changes to this project will be documented in this file.

## [v0.13.1][v0.13.1] (2015-09-29)
### Fixed
- Remove hardcoded source name when using (key,value) syntax [#1217](https://github.com/angular-ui/ui-select/pull/1217)
- Modify regex to accept a full 'collection expression' when not using (key,value) syntax [#1216](https://github.com/angular-ui/ui-select/pull/1216)
- Avoid to recalculate position when set 'down' [#1214](https://github.com/angular-ui/ui-select/issues/1214#issuecomment-144271352)

## [v0.13.0][v0.13.0] (2015-09-29)
### Added
- Allow to configure default dropdown position [#1213](https://github.com/angular-ui/ui-select/pull/1213) 
- Can use object as source with (key,value) syntax [#1208](https://github.com/angular-ui/ui-select/pull/1208) 
- CHANGELOG.md file created

### Changed
- Do not run bower after install automatically [#982](https://github.com/angular-ui/ui-select/pull/982)
- Avoid setting activeItem on mouseenter to improve performance [#1211](https://github.com/angular-ui/ui-select/pull/1211)

### Fixed
- Position dropdown UP or DOWN correctly depending on the available space [#1212](https://github.com/angular-ui/ui-select/pull/1212)
- Scroll to selected item [#976](https://github.com/angular-ui/ui-select/issues/976)
- Change `autocomplete='off'` to `autocomplete='false'` [#1210](https://github.com/angular-ui/ui-select/pull/1210)
- Fix to work correctly with debugInfoEnabled(false) [#1131](https://github.com/angular-ui/ui-select/pull/1131)
- Limit the maximum number of selections allowed in multiple mode [#1110](https://github.com/angular-ui/ui-select/pull/1110)

[v0.13.1]: https://github.com/angular-ui/ui-select/compare/v0.13.0...v0.13.1
[v0.13.0]: https://github.com/angular-ui/ui-select/compare/v0.12.1...v0.13.0
