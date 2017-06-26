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

    $("#qoo_iqas_params").val('');
    $("#qoo_iqas_params").parent().removeClass('is-dirty is-focused');

    $("#qoo_custom_params").val('');
    $("#qoo_custom_params").parent().removeClass('is-dirty is-focused');

    $("#qoo_constraints").prop('checked', false);
    $("#qoo_constraints").parent().removeClass('is-checked');
    showOrHideQoODiv();

    return false;
}

function sendIqasRequest(){
    alert($("#submit_request_form").serialize());
    return false;
}

$(function () {
    showOrHideQoODiv();

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
                    allHTML += '<label class="mdl-checkbox mdl-js-checkbox mdl-js-ripple-effect" for="checkbox-' + item['@id'] + '">';
                    allHTML += '<input id="checkbox-' + item['@id'] + '" class="mdl-checkbox__input" type="checkbox" name="checkbox-' + item['@id'] + '">';
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
