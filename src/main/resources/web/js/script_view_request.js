function hidePresentationText() {
    $("#presentation_card").addClass("mdl-cell--hide-phone mdl-cell--hide-tablet mdl-cell--hide-desktop");
}

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

function updateApplicationAndSensorNumber(request_id) {
    var bodyToSend = '{"sort":[{"@timestamp":{"order":"desc"}}],"size":20,"query":{"term":{"request_id":"' + request_id + '"}}}';
    $.ajax({
        type: "POST",
        contentType: 'application/json; charset=utf-8',
        crossDomain: true,
        data: bodyToSend,
        dataType: 'json',
        url: 'http://10.161.3.181:9200/logstash-*/_search',
        success: function (data) {
            if (data && data.hasOwnProperty('hits') && data.hits.hasOwnProperty('hits') && data.hits.hits.length > 0) {
                $.each(data.hits.hits, function (key, val) {
                    var oneRow = '<tr>';

                    var dateToPrint = new Date(parseInt(val['_source']['timestamps']['consumed']));
                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += dateToPrint.toString();
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val['_source']['obsStrValue'];
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val['_source']['provenance'];
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += '<p style="margin: 0px !important;">E2E: ';
                    oneRow += parseInt(val['_source']['timestamps']['consumed']) - parseInt(val['_source']['timestamps']['produced']);
                    oneRow += '</p>';
                    oneRow += '<p style="margin: 0px !important;">iQAS: ';
                    oneRow += parseInt(val['_source']['timestamps']['iQAS_out']) - parseInt(val['_source']['timestamps']['iQAS_in']);
                    oneRow += '</p>';
                    oneRow += '</td>';

                    if (val['_source'].hasOwnProperty('qoo') && Object.keys(val['_source']['qoo']).length > 0) {
                        oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                        $.each(val['_source']['qoo'], function (key2, val2) {
                            oneRow += '<p style="margin: 0px !important;">' + key2 + ": " + val2 + '</p>';
                        });
                        oneRow += '</td>';
                    }
                    else {
                        oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                        oneRow += 'N/A';
                        oneRow += '</td>';
                    }

                    if (val['_source'].hasOwnProperty('unit') && val['_source'].hasOwnProperty('quantityKind')) {
                        oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                        oneRow += val['_source']['quantityKind'].split('#')[1] + ' (' + val['_source']['unit'].split('#')[1] + ')';
                        oneRow += '</td>';
                    }
                    else {
                        oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                        oneRow += 'N/A';
                        oneRow += '</td>';
                    }

                    oneRow += '</tr>';

                    $("#table_latest_obs").append(oneRow);
                });
            }
            else {
                $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">No observations are available for this Request yet...</td></tr>');
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve latest observations for this Request...</td></tr>');
        }
    });
}

$(function () {

    var timeoutRetrieveRequests;

    var request_id = getURLParameter('request_id');
    if (request_id !== 'undefined') {
        $("#span_request_id").text(request_id);
    }

    (function worker() {

        $("#table_latest_obs").empty()
        $.ajax({
            dataType: "json",
            url: '/requests/' + request_id,
            success: function (data) {
                if (data && data.length > 0) {
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
                                $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Request has been removed...</td></tr>');
                            }
                            else {
                                updateApplicationAndSensorNumber(request_id);
                                timeoutRetrieveRequests = setTimeout(worker, 15000);
                            }
                        }
                    });
                }
                else {
                    clearTimeout(timeoutRetrieveRequests);
                    $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve latest observations for this Request...</td></tr>');
                }
            },
            error: function (jqXHR, textStatus, errorThrown) {
                clearTimeout(timeoutRetrieveRequests);
                $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve latest observations for this Request...</td></tr>');
            }
        });

    })();

});

