(function() {
	"use strict";
    $("#select").change(function() {
        var type = $(this).find(':selected').val();
        if(type === 'line') {
            console.log(type);
            $("#myChart1").show().siblings().hide();
        }
        if(type === 'bar') {
            console.log(type);
            $("#myChart2").show().siblings().hide();
        }
        if(type === 'pie') {
            console.log(type);
            $("#myChart3").show().siblings().hide();
        }
    });  


})();