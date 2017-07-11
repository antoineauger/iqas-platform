var chart;
var consumedDates = [];
var obsFreshnessValues = [];
var obsAccuracyValues = [];

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

function updateDataForRequest(request_id) {
    // Reset graph values
    consumedDates = [];
    obsFreshnessValues = [];
    obsAccuracyValues = [];

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
                    oneRow += '<p style="margin: 0 !important;">E2E: ';
                    oneRow += parseInt(val['_source']['timestamps']['consumed']) - parseInt(val['_source']['timestamps']['produced']);
                    oneRow += '</p>';
                    oneRow += '<p style="margin: 0 !important;">iQAS: ';
                    oneRow += parseInt(val['_source']['timestamps']['iQAS_out']) - parseInt(val['_source']['timestamps']['iQAS_in']);
                    oneRow += '</p>';
                    oneRow += '</td>';

                    if (val['_source'].hasOwnProperty('qoo') && Object.keys(val['_source']['qoo']).length > 0) {
                        oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                        $.each(val['_source']['qoo'], function (key2, val2) {
                            oneRow += '<p style="margin: 0 !important;">' + key2 + ": " + val2 + '</p>';
                        });
                        oneRow += '</td>';

                        consumedDates.push(moment(parseInt(val['_source']['timestamps']['consumed'])));
                        obsFreshnessValues.push(parseInt(val['_source']['qoo']['OBS_FRESHNESS']));
                        obsAccuracyValues.push(parseInt(val['_source']['qoo']['OBS_ACCURACY']));
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

                updateChartRendering(chart, consumedDates, obsFreshnessValues, obsAccuracyValues);
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

function clearGraphDatasets(chart, update) {
    consumedDates = [];
    obsFreshnessValues = [];
    obsAccuracyValues = [];
    if (update) {
        updateChartRendering(chart, consumedDates, obsFreshnessValues, obsAccuracyValues);
    }
}

function updateChartRendering(chart, consumedDates, obsFreshnessValues, obsAccuracyValues) {
    chart.data.labels = [];
    chart.data.datasets[0].data = [];
    chart.data.datasets[1].data = [];

    if (obsFreshnessValues.length > 0 || obsAccuracyValues.length > 0) {
        consumedDates.reverse().forEach(function (element) {
            chart.data.labels.push(element);
        });

        obsFreshnessValues.reverse().forEach(function (element) {
            chart.data.datasets[0].data.push(element);
        });

        obsAccuracyValues.reverse().forEach(function (element) {
            chart.data.datasets[1].data.push(element);
        });

        $("#view_request_qoo").removeClass("mdl-cell--hide-phone mdl-cell--hide-tablet mdl-cell--hide-desktop");
    }
    else {
        $("#view_request_qoo").addClass("mdl-cell--hide-phone mdl-cell--hide-tablet mdl-cell--hide-desktop");
    }

    chart.update();
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
                                clearGraphDatasets(chart, true);
                                $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Request has been removed...</td></tr>');
                                $("#id_kafka_topic").text('N/A');
                            }
                            else {
                                $("#id_kafka_topic").text(val.application_id + '_' + request_id);
                                updateDataForRequest(request_id);
                                timeoutRetrieveRequests = setTimeout(worker, 15000);
                            }
                        }
                    });
                }
                else {
                    clearTimeout(timeoutRetrieveRequests);
                    clearGraphDatasets(chart, true);
                    $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve latest observations for this Request...</td></tr>');
                }
            },
            error: function (jqXHR, textStatus, errorThrown) {
                clearGraphDatasets(chart, true);
                clearTimeout(timeoutRetrieveRequests);
                $("#table_latest_obs").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve latest observations for this Request...</td></tr>');
            }
        });

    })();

    var ctx = document.getElementById('myChart').getContext('2d');
    chart = new Chart(ctx, {
        // The type of chart we want to create
        type: 'line',

        // The data for our dataset
        data: {
            labels: [],
            datasets: [{
                label: "OBS_FRESHNESS",
                yAxisID: 'A',
                borderColor: 'rgb(96,125,139)',
                bezierCurve: false,
                lineTension: 0,
                data: []
            }, {
                label: "OBS_ACCURACY",
                yAxisID: 'B',
                borderColor: 'rgb(255,82,82)',
                backgroundColor: 'transparent',
                bezierCurve: false,
                lineTension: 0,
                data: []
            }]
        },

        // Configuration options go here
        options: {
            responsive: true,

            scales: {
                xAxes: [{
                    type: 'time',
                    display: true,
                    time: {
                        format: "HH:mm:ss A",
                        unit: 'second',
                        unitStepSize: 15
                    }
                }],
                yAxes: [{
                    id: 'A',
                    label: 'freshness (ms)',
                    type: 'linear',
                    position: 'left',
                    ticks: {
                        beginAtZero: true
                    }
                }, {
                    id: 'B',
                    label: 'accuracy (%)',
                    type: 'linear',
                    position: 'right',
                    ticks: {
                        beginAtZero: true
                    }
                }]
            }
        }
    });

});
