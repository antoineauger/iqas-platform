$(function(){
    $.ajax({
        url: "/pipelines?print=ids",
        success: function( result ) {
            $.each(result, function(index, item) {
                $( "#qoo_pipeline_list" ).append( "<li>" + item + "</li>" );
            });
        }
    });
});