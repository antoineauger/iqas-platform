$(function(){

    var timeoutRef;

    (function worker() {
        $.ajax({
            dataType: "json",
            url: '/requests',
            success: function (data) {
                var items = [];
                $.each(data, function (key, val) {
                    items.push("<li id='" + key + "'>" + val + "</li>");
                });

                if (items.length > 0) {
                    alert(items);
                }
                else {

                }
                timeoutRef = setTimeout(worker, 5000);
            },
            error: function( jqXHR, textStatus, errorThrown ) {
                clearTimeout(timeoutRef);
            }
        });
    })();

});