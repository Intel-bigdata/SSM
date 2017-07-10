# README

The size of the minimized [echarts distribution](https://github.com/ecomfe/echarts/blob/master/build/dist/echarts-all.js) is crazy. Not 100KB, not 500KB, it is 1MB! This will cause a cold page loading extremely slow. 

Dashing does not require all its features. So we provide a customized build (<400KB) with the minimal features that Dashing needs. 

If you are NOT familiar with AMD and echarts library, you can simply include 
`<script src='dashing-deps/echarts/2.2.7-compact/echarts-all.min.js></script>`
