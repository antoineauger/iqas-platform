var alreadyRegisteredSensors = [];
var pipelineDirectory = "";

function checkCorrectForm(){
    var isCorrect = true;
    if ($("#sensor_id").val() === '' ||
        $("#endpoint").val() === '' ||
        $("#interfaceDescription").val() === '') {
        isCorrect = false;
    }
    return isCorrect;
}

function askDeletionOfSensor(sensor_id) {
    var prefixes = 'PREFIX qoo: <http://isae.fr/iqas/qoo-ontology#>' +
        ' PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#>' +
        ' PREFIX iot-lite: <http://purl.oclc.org/NET/UNIS/fiware/iot-lite#>' +
        ' PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>';

    var query = ' DELETE { ?sensor ?p ?v } WHERE { ?sensor rdf:type ssn:Sensor . ?sensor iot-lite:id "' + sensor_id + '" . ?sensor ?p ?v }';

    var performOperation = confirm("Are you sure to delete " + sensor_id + "?");
    if (performOperation) {
        $.ajax({
            url: 'http://127.0.0.1:3030/qoo-onto/update',
            contentType: "application/sparql-update",
            data: prefixes + query,
            type: 'POST',
            processData: false,
            complete: function (jqXHR, textStatus) {
                updateSensorList();
            }
        });
    }
}

function askDeletionOfPipeline(pipeline_id) {
    alert("To delete " + pipeline_id + ", simply remove its corresponding .class file from the QoO Pipelines directory and refresh the page.");
}

function updateSensorList() {
    // Sensors
    $("#table_sensors_rows").empty();
    $.ajax({
        dataType: "json",
        url: '/sensors',
        success: function (data) {
            var htmlCodeToAppend = "";
            alreadyRegisteredSensors = [];

            $.each(data.sensors, function (key, val) {
                var oneRow = '<tr id="' + val["@id"].split('#')[1] + '">';

                oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                oneRow += '<a href="/sensors/' + val["@id"].split('#')[1] + '" target="_blank">' + val["@id"].split('#')[1] + '</a>' ;
                oneRow += '</td>';
                alreadyRegisteredSensors.push(val["@id"].split('#')[1]);

                oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                oneRow += val["observationValue"]["hasQuantityKind"].split('#')[1] ;
                oneRow += '</td>';

                oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                oneRow += val["observationValue"]["hasUnit"].split('#')[1] ;
                oneRow += '</td>';

                oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                oneRow += val["location"]["relative_location"] ;
                oneRow += '</td>';

                oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                oneRow += '<a href="#" onclick="askDeletionOfSensor(\'' + val["@id"].split('#')[1] + '\');"><i class="material-icons">delete</i></a>' ;
                oneRow += '</td>';

                oneRow += '</tr>';

                htmlCodeToAppend += oneRow;
            });

            if (htmlCodeToAppend !== "") {
                $("#table_sensors_rows").append(htmlCodeToAppend);
            }
            else {
                $("#table_sensors_rows").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">No Sensors are recorded yet...</td></tr>');
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#table_sensors_rows").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="6">Impossible to retrieve registered Sensors...</td></tr>');
        }
    });
}

function updatePipelineList() {
    $.ajax({
        type: "GET",
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        url: '/pipelines',
        success: function (data) {
            $("#table_pipelines_rows").empty();
            if (data.length > 0) {
                $.each(data, function(key, val) {

                    var oneRow = '<tr>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += '<a href="/pipelines/' + val["pipeline_id"] + '" target="_blank">' + val["pipeline_id"] + '</a>';
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val["pipeline_name"] ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val["pipeline_object"]["is_adaptable"] ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += val["pipeline_object"]["customizable_params"].toString() ;
                    oneRow += '</td>';

                    oneRow += '<td class="mdl-data-table__cell--non-numeric">';
                    oneRow += '<a href="#" onclick="askDeletionOfPipeline(\'' + val["pipeline_id"] + '\');"><i class="material-icons">delete</i></a>' ;
                    oneRow += '</td>';

                    $("#table_pipelines_rows").append(oneRow);
                });
            }
            else {
                $("#table_pipelines_rows").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="5">No QoO Pipelines available yet.</td></tr>');
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#table_pipelines_rows").empty();
            $("#table_pipelines_rows").append('<tr><td class="mdl-data-table__cell--non-numeric" colspan="5">Impossible to retrieve QoO Pipelines...</td></tr>');
        }
    });
}

function clearForm() {
    $("#sensor_id").val('');
    $("#sensor_id").parent().removeClass('is-dirty is-focused');

    $("#endpoint").val('');
    $("#endpoint").parent().removeClass('is-dirty is-focused');

    $("#interfaceDescription").val('');
    $("#interfaceDescription").parent().removeClass('is-dirty is-focused');

    $("#minValue").val('');
    $("#minValue").parent().removeClass('is-dirty is-focused');

    $("#maxValue").val('');
    $("#maxValue").parent().removeClass('is-dirty is-focused');

    $('#location option[value="qoo:loc_portet"]').prop('selected', true);
    $('#topic option[value="qoo:temperature"]').prop('selected', true);
    $('#quantityKind option[value="m3-lite:Temperature"]').prop('selected', true);
    $('#unit option[value="m3-lite:DegreeCelsius"]').prop('selected', true);
}

function clearForm2() {
    $("#pipelineTextarea").val('');
    $("#pipelineTextarea").parent().removeClass('is-dirty is-focused');
}

function postNewSensor() {
    var sensor_id = $("#sensor_id").val();
    var topic = $("#topic").val();
    var topic_id = topic.split(':')[1];
    var location = $("#location").val();
    var quantityKind = $("#quantityKind").val();
    var unit = $("#unit").val();
    var endpoint = $("#endpoint").val();
    var interfaceDescription = $("#interfaceDescription").val();
    var interfaceType = $("#interfaceType").val();
    var minValue = $("#minValue").val();
    var maxValue = $("#maxValue").val();

    var prefixes = 'PREFIX qoo: <http://isae.fr/iqas/qoo-ontology#>' +
        ' PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#>' +
        ' PREFIX iot-lite: <http://purl.oclc.org/NET/UNIS/fiware/iot-lite#>' +
        ' PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>' +
        ' PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>' +
        ' PREFIX owl: <http://www.w3.org/2002/07/owl#>' +
        ' PREFIX m3-lite: <http://purl.org/iot/vocab/m3-lite#>';

    var query = 'INSERT DATA {';

    query += '<http://isae.fr/iqas/qoo-ontology#' + sensor_id + '> rdf:type ssn:Sensor ;' +
                'iot-lite:id "' + sensor_id + '" ;' +
                'geo:location ' + location + ' ;' +
                'qoo:sensorStateValue "CONNECTED" ;' +
                'ssn:madeObservation qoo:obs_' + sensor_id + ' ;' +
                'iot-lite:exposedBy qoo:serv_' + sensor_id + ' ;' +
                'ssn:hasMeasurementCapability qoo:capa_' + sensor_id + ' .';

    query += '<http://isae.fr/iqas/qoo-ontology#obs_' + sensor_id + '> ssn:observationResult qoo:out_' + sensor_id + ' ;' +
                'ssn:observedProperty ' + topic + ' .';

    query += '<http://isae.fr/iqas/qoo-ontology#' + topic_id + '> rdf:type ssn:Property ;' +
                'qoo:canBeRetrievedThrough qoo:serv_' + sensor_id + ' ;' +
                'ssn:isPropertyOf qoo:publicLocations .';

    query += '<http://isae.fr/iqas/qoo-ontology#out_' + sensor_id + '> iot-lite:hasValue qoo:ex_obs_out_' + sensor_id + ' .';

    query += '<http://isae.fr/iqas/qoo-ontology#ex_obs_out_' + sensor_id + '> rdf:type ssn:ObservationValue ;' +
                'iot-lite:hasUnit qoo:unit_' + sensor_id + ' ;' +
                'iot-lite:hasQuantityKind qoo:quantityKind_' + sensor_id + ' .';

    query += '<http://isae.fr/iqas/qoo-ontology#unit_' + sensor_id + '> rdf:type ' + unit + ' .';

    query += '<http://isae.fr/iqas/qoo-ontology#quantityKind_' + sensor_id + '> rdf:type ' + quantityKind + ' .';

    query += '<http://isae.fr/iqas/qoo-ontology#capa_' + sensor_id + '> rdf:type ssn:hasMeasurementCapability ;' +
                'ssn:hasMeasurementProperty qoo:range_' + sensor_id + ' ;' +
                'ssn:forProperty ' + topic + ' .';

    query += ' <http://isae.fr/iqas/qoo-ontology#range_' + sensor_id + '> rdf:type ssn:MeasurementRange ;' +
                'qoo:hasMinValue "' + minValue + '" ;' +
                'qoo:hasMaxValue "' + maxValue + '" .';

    query += ' <http://isae.fr/iqas/qoo-ontology#serv_' + sensor_id + '> rdf:type iot-lite:Service ;' +
                'iot-lite:interfaceType "' + interfaceType + '" ;' +
                'iot-lite:endpoint "' + endpoint + '" ;' +
                'iot-lite:interfaceDescription "' + interfaceDescription + '" .';

    query += '}';

    if ($.inArray(sensor_id, alreadyRegisteredSensors) > -1) {
        alert("Sensor IDs should be unique!");
    }
    else if (checkCorrectForm()) {
        alert(prefixes + query);
        alert(alreadyRegisteredSensors);

        $.ajax({
            url: 'http://127.0.0.1:3030/qoo-onto/update',
            contentType: "application/sparql-update",
            data: prefixes + query,
            type: 'POST',
            processData: false,
            success: function (data) {
                clearForm();
                updateSensorList();
            },
            error: function (jqXHR, textStatus, errorThrown) {
                alert("An error has occurred! Reason: " + errorThrown);
            }
        });
    }
    else {
        alert("All fields are mandatory!");
    }
}

function postNewPipeline() {
    var payload = $("#pipelineTextarea").val();
    if (payload !== '') {
        $.ajax({
            url: 'http://127.0.0.1:3030/qoo-onto/update',
            contentType: "application/sparql-update",
            data: payload,
            type: 'POST',
            processData: false,
            success: function (data) {
                clearForm2();
                alert("You should now place the corresponding pipeline in the directory " + pipelineDirectory + ' and refresh the page to see your QoO Pipeline in the list.');
            },
            error: function (jqXHR, textStatus, errorThrown) {
                alert("An error has occurred! Reason: " + errorThrown);
            }
        });
    }
    else {
        alert("Cannot submit an empty SPARQL request. If you don't know where to start, give a try to the button 'LOAD TEMPLATE'.");
    }
}

function loadPipelineTemplate() {
    $("#pipelineTextarea").parent().addClass('is-dirty');

    var prefixes = 'PREFIX qoo: <http://isae.fr/iqas/qoo-ontology#>\n' +
        'PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#>\n' +
        'PREFIX iot-lite: <http://purl.oclc.org/NET/UNIS/fiware/iot-lite#>\n' +
        'PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n' +
        'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n' +
        'PREFIX owl: <http://www.w3.org/2002/07/owl#>\n' +
        'PREFIX m3-lite: <http://purl.org/iot/vocab/m3-lite#>\n\n';

    var query = 'INSERT DATA {\n';
    query += '   <http://isae.fr/iqas/qoo-ontology#iQAS> qoo:provides qoo:MY_PIPELINE .\n\n';

    query += '   <http://isae.fr/iqas/qoo-ontology#MY_PIPELINE> rdf:type qoo:QoOPipeline ;\n';
    query += '      qoo:allowsToSet qoo:MY_PARAMETER1 .\n\n';

    query += '   <http://isae.fr/iqas/qoo-ontology#MY_PARAMETER1> rdf:type qoo:QoOCustomizableParameter ;\n';
    query += '      qoo:documentation "Documentation for MY_PARAMETER1" ;\n';
    query += '      qoo:paramType "Integer" ;\n';
    query += '      qoo:paramMinValue "0" ;\n';
    query += '      qoo:paramMaxValue "+INF" ;\n';
    query += '      qoo:paramInitialValue "1" ;\n';
    query += '      qoo:has qoo:MY_PARAMETER1_EFFECT1 ;\n';
    query += '      qoo:has qoo:MY_PARAMETER1_EFFECT2 .\n\n';

    query += '   <http://isae.fr/iqas/qoo-ontology#MY_PARAMETER1_EFFECT1> rdf:type qoo:QoOEffect ;\n';
    query += '      qoo:paramVariation "HIGH" ;\n';
    query += '      qoo:qooAttributeVariation "HIGH" ;\n';
    query += '      qoo:impacts qoo:OBS_RATE .\n\n';

    query += '   <http://isae.fr/iqas/qoo-ontology#MY_PARAMETER1_EFFECT2> rdf:type qoo:QoOEffect ;\n';
    query += '      qoo:paramVariation "CONSTANT" ;\n';
    query += '      qoo:qooAttributeVariation "LOW" ;\n';
    query += '      qoo:impacts qoo:OBS_FRESHNESS .\n\n';

    query += '}';

    $("#pipelineTextarea").val(prefixes + query);
}

$(function(){
    var myRegexp = /qoo_pipelines_dir=(.*)/g;

    updateSensorList();
    updatePipelineList();

    $.ajax({
        url: "/configuration/iqas",
        success: function( result ) {
            var match = myRegexp.exec(result);
            pipelineDirectory = match[1];
            $( "#iqas-config" ).text( result );
        }
    });

    $.ajax({
        url: "/configuration/ontologies",
        success: function( result ) {
            $( "#onto-config" ).text( result );
        }
    });

    $("#location").focusin(function(){
        $("#info_form_location").css("display", "block");
    });
    $("#location").focusout(function(){
        $("#info_form_location").css("display", "none");
    });

    $("#topic").focusin(function(){
        $("#info_form_topic").css("display", "block");
    });
    $("#topic").focusout(function(){
        $("#info_form_topic").css("display", "none");
    });

    $("#quantityKind").focusin(function(){
        $("#info_form_quantityKind").css("display", "block");
    });
    $("#quantityKind").focusout(function(){
        $("#info_form_quantityKind").css("display", "none");
    });

    $("#unit").focusin(function(){
        $("#info_form_unit").css("display", "block");
    });
    $("#unit").focusout(function(){
        $("#info_form_unit").css("display", "none");
    });

    $("#interfaceType").focusin(function(){
        $("#info_form_interfaceType").css("display", "block");
    });
    $("#interfaceType").focusout(function(){
        $("#info_form_interfaceType").css("display", "none");
    });

    $("#minValue").focusin(function(){
        $("#info_form_minValue").css("display", "block");
    });
    $("#minValue").focusout(function(){
        $("#info_form_minValue").css("display", "none");
    });

    $("#maxValue").focusin(function(){
        $("#info_form_maxValue").css("display", "block");
    });
    $("#maxValue").focusout(function(){
        $("#info_form_maxValue").css("display", "none");
    });
});