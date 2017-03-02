(function() {
	"use strict";
  var myChart = echarts.init(document.getElementById('myChart1'));

    //指定图表的配置项和数据 
    var option = { 
        title: { 
            //显示标题
            text: 'Cache统计',
            //标题显示的位置
            left: 'center' 
        }, 
    
    tooltip: {},
    //在示例中我们看到图表中需要的数据为
    //显示需要统计的分类
    xAxis: [
        {
       data: (function() {
             var arr1 = [];
             $.ajax({
                 type:"GET",
                 url : 'http://localhost:9871/ssm/v1?op=SHOWCACHE',
                 dataType:"json",
                 async: false,
                 success : function(result) {
                     for (var key in result) {
                         arr1.push(key);
                     }

                 },
                 error : function(errorMsg) {
                     alert("sorry, 请求数据失败");
                     myChart.hideLoading();
                 }
             })

             return arr1;
        })()

        }
    ], 
    
    yAxis: {}, 
    series: [{ 
        name: '数量', 
        type: 'line',
//        data : [3,2,1,33]
        data: (function() {
            var arr1 = [];
            $.ajax({
                type:"GET",
                url : 'http://localhost:9871/ssm/v1?op=SHOWCACHE',
                dataType:"json",
                async: false,
                success : function(result) {
                    for (var key in result) {
                        arr1.push(result[key]);
                    }

                },
                error : function(errorMsg) {
                    alert("sorry, 请求数据失败");
                    myChart.hideLoading();
                }
            })
            return arr1;
        })()
    }]

}; 
    
    
// 使用刚指定的配置项和数据显示图表。 
myChart.setOption(option);     



})();