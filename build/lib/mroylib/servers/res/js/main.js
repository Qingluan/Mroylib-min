
$("div#new-user").hover(
    function(){
	var checked = $(this).attr("moved");
	if (checked == "un"){	
            $(this).animate({
	        'left' :"-=30%"
	    },1000,function(){
	        $("div#user-panel").collapse('toggle').attr("moved","ok");
	    }).attr("moved","ok");
	}
    }
);

$("div#user-panel").hover(
    null,
    function (){
    	var checked = $("div#new-user").attr("moved");
	if (checked == "ok"){
	    $("div#new-user").animate({
	    	'left':'+=30%'	
	    },800,function(){
	    	$("div#user-panel").collapse("toggle").attr("moved","un");
	    }).attr("moved","un");
	}
    }
);

