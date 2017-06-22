function getURLParameter(sParam) {
    var sPageURL = window.location.search.substring(1);
    var sURLVariables = sPageURL.split('&');
    for (var i = 0; i < sURLVariables.length; i++) {
        var sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] === sParam) {
            return sParameterName[1];
        }
    }
    return 'undefined';
}

$(function () {

    var request_id = getURLParameter('request_id');
    if (request_id !== 'undefined') {
        $("#span_request_id").text(request_id);
    }
    else {

    }

});

