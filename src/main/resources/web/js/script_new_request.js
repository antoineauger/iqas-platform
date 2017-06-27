function showOrHideQoODiv() {
    if ($("#qoo_constraints").is(':checked')) {
        $("#div_qoo_constraints").css("display", "block");
    }
    else {
        $("#div_qoo_constraints").css("display", "none");
    }
}

function clearForm(){
    $("#application_id").val('');
    $("#application_id").parent().removeClass('is-dirty is-focused');

    $("#topic").val('');
    $("#topic").parent().removeClass('is-dirty is-focused');

    $("#location").val('');
    $("#location").parent().removeClass('is-dirty is-focused');

    $('#sla option[value=sla-BEST_EFFORT]').prop('selected', true);

    $('form input[type=checkbox]').each(function (index, element) {
        $(element).parent().removeClass('is-checked');
        $(element).prop('checked', false);
    })
    componentHandler.upgradeDom('MaterialCheckbox');

    $('#obs_level option[value=obs-level-RAW_DATA]').prop('selected', true);

    $("#qoo-iqas_params").val('');
    $("#qoo-iqas_params").parent().removeClass('is-dirty is-focused');

    $("#qoo-custom_params").val('');
    $("#qoo-custom_params").parent().removeClass('is-dirty is-focused');

    $("#qoo_constraints").prop('checked', false);
    $("#qoo_constraints").parent().removeClass('is-checked');
    showOrHideQoODiv();

    return false;
}

function checkCorrectForm(){
    var isCorrect = true;
    if ($("#application_id").val() === '' ||
        $("#topic").val() === '' ||
        $("#location").val() === '') {
        isCorrect = false;
    }

    if ($("#qoo_constraints").is(':checked')) {
        try {
            if ($("#qoo-iqas_params").val() !== '') {
                var dummyResult1 = JSON.parse($("#qoo-iqas_params").val());
            }
            if ($("#qoo-custom_params").val() !== '') {
                var dummyResult2 = JSON.parse($("#qoo-custom_params").val());
            }
        }
        catch (e) {
            isCorrect = false;
        }
    }
    return isCorrect;
}

function sendIqasRequest(){
    if (checkCorrectForm()) {
        var dataJSON = $("#submit_request_form").serializeArray();
        var properParamsToSend = {};
        var interestedInParams = [];
        var qooParams = {};
        dataJSON.forEach(function (item) {
            if (item.name !== 'qoo_constraints' && item.value !== '') { // Do not include the checkbox value
                if (item.name.match("^qoo-")) {
                    var qooParamName = item.name.split("-")[1];
                    if (item.name.match("^qoo-OBS_") && item.value === 'on') {
                        interestedInParams.push(qooParamName);
                    }
                    else {
                        qooParams[qooParamName] = item.value;
                    }
                }
                else {
                    properParamsToSend[item.name] = item.value;
                }
            }
        });

        qooParams['interested_in'] = interestedInParams;
        if ($("#qoo-iqas_params").val() !== '') {
            qooParams['iqas_params'] = JSON.parse($("#qoo-iqas_params").val());
        }
        if ($("#qoo-custom_params").val() !== '') {
            qooParams['custom_params'] = JSON.parse($("#qoo-custom_params").val());
        }
        properParamsToSend['qoo'] = qooParams;

        $.ajax({
            type: "POST",
            contentType: 'application/json; charset=utf-8',
            dataType: 'json',
            data: JSON.stringify(properParamsToSend),
            url: '/requests',
            success: function (iQASResponse) {
                window.location.href = "/viewRequest?request_id=" + iQASResponse.request_id;
            },
            error: function (jqXHR, textStatus, errorThrown) {
                alert(errorThrown);
            }
        });
    }
    else {
        alert("The form is not correctly filled, please check the supplied inputs.");
    }
    return false;
}

$(function () {
    showOrHideQoODiv();

    $("#location").focusin(function(){
        $("#info_form_location").css("display", "block");
    });
    $("#location").focusout(function(){
        $("#info_form_location").css("display", "none");
    });

    $("#qoo-iqas_params").focusin(function(){
        $("#info_form_iqas_params").css("display", "block");
    });
    $("#qoo-iqas_params").focusout(function(){
        $("#info_form_iqas_params").css("display", "none");
    });

    $("#qoo-custom_params").focusin(function(){
        $("#info_form_custom_params").css("display", "block");
    });
    $("#qoo-custom_params").focusout(function(){
        $("#info_form_custom_params").css("display", "none");
    });

    $.ajax({
        type: "GET",
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        url: '/qoo/attributes',
        success: function (data) {
            $("#interested_in_checkboxes").empty();
            if (data.qoo_attributes.length > 0) {
                var allHTML = "";
                $.each(data.qoo_attributes, function(i, item) {
                    allHTML += '<label class="mdl-checkbox mdl-js-checkbox mdl-js-ripple-effect" for="qoo-' + item['@id'] + '">';
                    allHTML += '<input id="qoo-' + item['@id'] + '" class="mdl-checkbox__input" type="checkbox" name="qoo-' + item['@id'] + '">';
                    allHTML += '<span class="mdl-checkbox__label">' + item['@id'] + '</span>';
                    allHTML += '</label>';
                });

                $("#interested_in_checkboxes").append(allHTML);
                componentHandler.upgradeDom('MaterialCheckbox');
            }
            else {
                $("#interested_in_checkboxes").append("No QoO Attributes available yet.");
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#interested_in_checkboxes").empty();
            $("#interested_in_checkboxes").append("Impossible to retrieve QoO Attributes...");
        }
    });
});
