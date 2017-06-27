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

function extractStartTime(logs) {
    var regex = /(.*):/g;
    var tempDate = logs[logs.length-1].match(regex).toString();
    tempDate = tempDate.substring(0, tempDate.length - 1);
    return tempDate;
}

$(function () {

    var timeoutRetrieveRequests;

    var request_id = getURLParameter('request_id');
    if (request_id !== 'undefined') {
        $("#span_request_id").text(request_id);
    }

    (function worker() {

        $("#table_requests_rows").empty()
        $.ajax({
            dataType: "json",
            url: '/requests/' + request_id,
            success: function (data) {
                $.each(data, function (key, val) {
                    if (val.request_id === request_id) {
                        $("#rai_date").text(extractStartTime(val.logs));
                        $("#rai_appli").text(val.application_id);
                        $("#rai_topic").text(val.topic);
                        $("#rai_location").text(val.location);
                        $("#rai_obs_level").text(val.obs_level);
                        $("#rai_curr_status").text(val.current_status);

                        if (val.current_status === 'REMOVED' || val.current_status === 'REJECTED') {
                            clearTimeout(timeoutRetrieveRequests);
                        }
                        else {
                            timeoutRetrieveRequests = setTimeout(worker, 15000);
                        }
                    }
                });
            },
            error: function (jqXHR, textStatus, errorThrown) {
                clearTimeout(timeoutRetrieveRequests);
            }
        });

    })();

});

