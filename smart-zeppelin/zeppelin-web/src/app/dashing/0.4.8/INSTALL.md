# Installation

+ Install Dashing and dependencies with [npm](https://nodejs.org/).

``` bash
$ npm install dashing

# dependencies
$ npm install bootstrap \
    angular angular-animate angular-sanitize \
    angular-motion angular-strap bootstrap-additions \
    angular-ui-select \
    angular-smart-table \
    echarts

# alternatively we provide a minimal echarts build and roboto fonts
$ npm install dashing-deps
```

+ Include the following lines in your `index.html`. *Note that the paths of the vendor libraries should be fixed manually for your own environment.*

``` html
<!-- styles (keep files in this order) -->
<link rel="stylesheet" href="vendors/bootstrap/bootstrap.min.css"/>
<link rel="stylesheet" href="vendors/bootstrap-additions/bootstrap-additions.min.css"/>
<link rel="stylesheet" href="vendors/angular-motion/angular-motion.min.css"/>
<link rel="stylesheet" href="vendors/angular-ui-select/select.min.css"/>
<link rel="stylesheet" href="vendors/dashing/dashing.min.css"/>
<!-- libraries -->
<script src="vendors/angular/angular.min.js"></script>
<script src="vendors/angular-animate/angular-animate.min.js"></script>
<script src="vendors/angular-sanitize/angular-sanitize.min.js"></script>
<script src="vendors/angular-strap/angular-strap.min.js"></script>
<script src="vendors/angular-strap/angular-strap.tpl.min.js"></script>
<script src="vendors/angular-smart-table/smart-table.min.js"></script>
<script src="vendors/angular-ui-select/select.min.js"></script>
<script src="vendors/dashing-deps/echarts/echart-all.min.js"></script>
<script src="vendors/dashing/dashing.min.js"></script>
```

+ Inject the `dashing` into your app:

``` js
angular.module('myApp', ['dashing']);
```

