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
    var tempDate = logs[logs.length-1].match(regex).toString();
    tempDate = tempDate.substring(0, tempDate.length - 1);
    return tempDate;
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
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#nb_applications").text("0");
            $("#nb_sensors").text("0");
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
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#nb_containers").text("0");
        }
    });
}

function updateQoOPipelines() {
    $.ajax({
        type: "GET",
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        url: '/pipelines?print=ids',
        success: function (data) {
            $("#infos_qoo_pipelines").find(".mdl-card__supporting-text").empty();
            if (data.length > 0) {
                var allHTML = '<ul class="demo-list-icon mdl-list">';
                $.each(data, function(i, item) {
                    allHTML += '<li class="mdl-list__item">';
                    allHTML += '<span class="mdl-list__item-primary-content">';
                    allHTML += '<i class="material-icons mdl-list__item-icon">linear_scale</i>';
                    allHTML += '<a href="/pipelines/' + item + '" target="_blank">';
                    allHTML += item;
                    allHTML += '</a>';
                    allHTML += '</span>';
                    allHTML += '</li>';
                });
                allHTML += '</ul>';

                $("#infos_qoo_pipelines").find(".mdl-card__supporting-text").append(allHTML);
            }
            else {
                $("#infos_qoo_pipelines").find(".mdl-card__supporting-text").append("No QoO Pipelines available yet.");
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#infos_qoo_pipelines").find(".mdl-card__supporting-text").empty();
            $("#infos_qoo_pipelines").find(".mdl-card__supporting-text").append("Impossible to retrieve QoO Pipelines...");
        }
    });
}

$(function () {

    var timeoutRetrieveRequests;

    (function worker() {
        // Elasticsearch
        updateApplicationAndSensorNumber();
        updateContainerNumber();
        updateQoOPipelines();

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
                    oneRow += extractStartTime(val.logs);
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += '<a href=""></a>';
                    if (val.current_status === 'ENFORCED') {
                        oneRow += '<a href="/viewRequest?request_id=' + val.request_id + '">';
                    }
                    oneRow += val.request_id ;
                    if (val.current_status === 'ENFORCED') {
                        oneRow += '</a>';
                    }
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
                    oneRow += '</tr>';

                    items.push(oneRow);
                });

                if (items.length > 0) {
                    for (var key in items.reverse()) {
                        $("#table_requests_rows").append(items[key]);
                    }
                }
                else {
                    $("#table_requests_rows").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">No requests have been submitted yet...</td></tr>');
                }
            },
            error: function (jqXHR, textStatus, errorThrown) {
                $("#table_requests_rows").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve iQAS requests...</td></tr>');
            },
            complete: function (jqXHR, textStatus) {
                timeoutRetrieveRequests = setTimeout(worker, 15000);
            }
        });

    })();

});

