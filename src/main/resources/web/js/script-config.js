$(function(){
    /*$.ajax({
        url: "/pipelines?print=ids",
        success: function( result ) {
            $.each(result, function(index, item) {
                $( "#qoo_pipeline_list" ).append( "<li>" + item + "</li>" );
            });
        }
    });*/

    $.ajax({
        url: "/configuration/iqas",
        success: function( result ) {
            $( "#iqas-config" ).text( result );
        }
    });

    $.ajax({
        url: "/configuration/ontologies",
        success: function( result ) {
            $( "#onto-config" ).text( result );
        }
    });
});