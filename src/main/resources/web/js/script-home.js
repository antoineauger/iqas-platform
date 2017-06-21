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

$(function () {

    var timeoutRef;
    var image_iqas_nok_status = new Image();
    var image_iqas_ok_status = new Image();

    (function worker() {
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

