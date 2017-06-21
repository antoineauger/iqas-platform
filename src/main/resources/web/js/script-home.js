function hidePresentationText() {
    $("#presentation_card").addClass("mdl-cell--hide-phone mdl-cell--hide-tablet mdl-cell--hide-desktop");
}

function askDeletionOfRequest(request_id) {
    $.ajax({
        type: 'DELETE',
        url: '/requests/' + request_id,
        complete: function (jqXHR, textStatus) {
            $("#deleteLinkRequest" + request_id).remove();
        }
    });
}

function extractStartTime(logs) {
    var regex = /(.*):/g;
    return logs[logs.length-1].match(regex);
}

function updateApplicationAndSensorNumber() {
    var bodyToSend = '{"size":0,"query":{"exists":{"field":"application_id.keyword"}},"aggs":{"ze_countAppli":{"cardinality":{"field":"application_id.keyword"}},"ze_countSensor":{"cardinality":{"field":"provenance.keyword"}}}}';
    $.ajax({
        type: "POST",
        contentType: 'application/json; charset=utf-8',
        crossDomain: true,
        data: bodyToSend,
        dataType: 'json',
        url: 'http://10.161.3.181:9200/logstash-*/_search',
        success: function (data) {
            if (data.hasOwnProperty('aggregations') && data.aggregations.hasOwnProperty('ze_countAppli')) {
                $("#nb_applications").text(data.aggregations.ze_countAppli.value);
            }
            else {
                $("#nb_applications").text("0");
            }

            if (data.hasOwnProperty('aggregations') && data.aggregations.hasOwnProperty('ze_countSensor')) {
                $("#nb_sensors").text(data.aggregations.ze_countSensor.value);
            }
            else {
                $("#nb_sensors").text("0");
            }
        }
    });
}

function updateContainerNumber() {
    var bodyToSend = '{"size":0,"query":{"exists":{"field":"containerName"}},"aggs":{"ze_count":{"cardinality":{"field":"containerName"}}}}';
    $.ajax({
        type: "POST",
        contentType: 'application/json; charset=utf-8',
        crossDomain: true,
        data: bodyToSend,
        dataType: 'json',
        url: 'http://10.161.3.181:9200/dockbeat-*/_search',
        success: function (data) {
            if (data.hasOwnProperty('aggregations')) {
                $("#nb_containers").text(data.aggregations.ze_count.value);
            }
            else {
                $("#nb_containers").text("0");
            }
        }
    });
}

$(function () {

    var timeoutRef;
    var image_iqas_nok_status = new Image();
    var image_iqas_ok_status = new Image();

    (function worker() {
        // Elasticsearch
        updateApplicationAndSensorNumber();
        updateContainerNumber();

        // Requests
        $("#table_requests_rows").empty()
        $.ajax({
            dataType: "json",
            url: '/requests',
            success: function (data) {
                var items = [];

                $.each(data, function (key, val) {
                    var oneRow = '<tr>';
                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    var tempDate = extractStartTime(val.logs).toString();
                    tempDate = tempDate.substring(0, tempDate.length - 1);
                    oneRow += tempDate ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += '<a href=""></a>';
                    oneRow += val.request_id ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val.application_id ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val.obs_level ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val.current_status ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    var id = "deleteLinkRequest" + val.request_id;
                    if (val.current_status === 'ENFORCED') {
                        oneRow += '<a id="deleteLinkRequest' + val.request_id + '" href="#" onclick="askDeletionOfRequest(\'' + val.request_id + '\');"><i class="material-icons">delete</i></a>' ;
                    }
                    oneRow += '</td>';

                    items.push(oneRow);
                });

                if (items.length > 0) {
                    for (var key in items.reverse()) {
                        $("#table_requests_rows").append(items[key]);
                    }
                }
                else {
                    $("#table_requests_rows").append(' <td class="mdl-data-table__cell--non-numeric" colspan="6">No requests have been submitted yet...</td>');
                }

                image_iqas_ok_status.src = '/figures/iqas_status_ok.png';
                image_iqas_nok_status.src = '/figures/iqas_status_nok.png';

                $("#iqas_status_icon").attr("src", "/figures/iqas_status_ok.png");
                $("#iqas_status_icon").attr("title", "iQAS platform is currently running")
            },
            error: function (jqXHR, textStatus, errorThrown) {
                $("#iqas_status_icon").attr("src", "/figures/iqas_status_nok.png");
                $("#iqas_status_icon").attr("title", "Error: make sure that the iQAS platform is running")
                $("#table_requests_rows").append(' <td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve iQAS requests...</td>');
            },
            complete: function (jqXHR, textStatus) {
                timeoutRef = setTimeout(worker, 15000);
            }
        });

    })();

});

