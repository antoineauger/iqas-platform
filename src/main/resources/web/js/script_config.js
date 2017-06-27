$(function(){
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