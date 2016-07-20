/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* global nf, d3 */

nf.ConnectionConfiguration = (function () {

    var CONNECTION_OFFSET_Y_INCREMENT = 75;
    var CONNECTION_OFFSET_X_INCREMENT = 200;

    var config = {
        urls: {
            api: '../nifi-api',
            prioritizers: '../nifi-api/flow/prioritizers'
        }
    };

    /**
     * Removes the temporary if necessary.
     */
    var removeTempEdge = function () {
        d3.select('path.connector').remove();
    };

    /**
     * Initializes the source in the new connection dialog.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceNewConnectionDialog = function (source) {
        // handle the selected source
        if (nf.CanvasUtils.isProcessor(source)) {
            return $.Deferred(function (deferred) {
                // initialize the source processor
                initializeSourceProcessor(source).done(function (processor) {
                    if (!nf.Common.isEmpty(processor.relationships)) {
                        // populate the available connections
                        $.each(processor.relationships, function (i, relationship) {
                            createRelationshipOption(relationship.name);
                        });

                        // if there is a single relationship auto select
                        var relationships = $('#relationship-names').children('div');
                        if (relationships.length === 1) {
                            relationships.children('div.available-relationship').removeClass('checkbox-unchecked').addClass('checkbox-checked');
                        }

                        // configure the button model
                        $('#connection-configuration').modal('setButtonModel', [{
                            buttonText: 'Add',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
                            handler: {
                                click: function () {
                                    // get the selected relationships
                                    var selectedRelationships = getSelectedRelationships();

                                    // ensure some relationships were selected
                                    if (selectedRelationships.length > 0) {
                                        addConnection(selectedRelationships);
                                    } else {
                                        // inform users that no relationships were selected
                                        nf.Dialog.showOkDialog({
                                            headerText: 'Connection Configuration',
                                            dialogContent: 'The connection must have at least one relationship selected.'
                                        });
                                    }

                                    // close the dialog
                                    $('#connection-configuration').modal('hide');
                                }
                            }
                        },
                            {
                                buttonText: 'Cancel',
                                color: {
                                    base: '#E3E8EB',
                                    hover: '#C7D2D7',
                                    text: '#004849'
                                },
                                handler: {
                                    click: function () {
                                        $('#connection-configuration').modal('hide');
                                    }
                                }
                            }]);

                        // resolve the deferred
                        deferred.resolve();
                    } else {
                        // there are no relationships for this processor
                        nf.Dialog.showOkDialog({
                            headerText: 'Connection Configuration',
                            dialogContent: '\'' + nf.Common.escapeHtml(processor.name) + '\' does not support any relationships.'
                        });

                        // reset the dialog
                        resetDialog();

                        deferred.reject();
                    }
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        } else {
            return $.Deferred(function (deferred) {
                // determine how to initialize the source
                var connectionSourceDeferred;
                if (nf.CanvasUtils.isInputPort(source)) {
                    connectionSourceDeferred = initializeSourceInputPort(source);
                } else if (nf.CanvasUtils.isRemoteProcessGroup(source)) {
                    connectionSourceDeferred = initializeSourceRemoteProcessGroup(source);
                } else if (nf.CanvasUtils.isProcessGroup(source)) {
                    connectionSourceDeferred = initializeSourceProcessGroup(source);
                } else {
                    connectionSourceDeferred = initializeSourceFunnel(source);
                }

                // finish initialization when appropriate
                connectionSourceDeferred.done(function () {
                    // configure the button model
                    $('#connection-configuration').modal('setButtonModel', [{
                        buttonText: 'Add',
                        color: {
                            base: '#728E9B',
                            hover: '#004849',
                            text: '#ffffff'
                        },
                        handler: {
                            click: function () {
                                // add the connection
                                addConnection();

                                // close the dialog
                                $('#connection-configuration').modal('hide');
                            }
                        }
                    },
                        {
                            buttonText: 'Cancel',
                            color: {
                                base: '#E3E8EB',
                                hover: '#C7D2D7',
                                text: '#004849'
                            },
                            handler: {
                                click: function () {
                                    $('#connection-configuration').modal('hide');
                                }
                            }
                        }]);

                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        }
    };

    /**
     * Initializes the source when the source is an input port.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceInputPort = function (source) {
        return $.Deferred(function (deferred) {
            // get the input port data
            var inputPortData = source.datum();

            // populate the port information
            $('#input-port-source').show();
            $('#input-port-source-name').text(inputPortData.component.name);

            // populate the connection source details
            $('#connection-source-id').val(inputPortData.id);
            $('#connection-source-component-id').val(inputPortData.id);

            // populate the group details
            $('#connection-source-group-id').val(nf.Canvas.getGroupId());
            $('#connection-source-group-name').text(nf.Canvas.getGroupName());

            // resolve the deferred
            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the source when the source is an input port.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceFunnel = function (source) {
        return $.Deferred(function (deferred) {
            // get the funnel data
            var funnelData = source.datum();

            // populate the port information
            $('#funnel-source').show();

            // populate the connection source details
            $('#connection-source-id').val(funnelData.id);
            $('#connection-source-component-id').val(funnelData.id);

            // populate the group details
            $('#connection-source-group-id').val(nf.Canvas.getGroupId());
            $('#connection-source-group-name').text(nf.Canvas.getGroupName());

            // resolve the deferred
            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the source when the source is a processor.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceProcessor = function (source) {
        return $.Deferred(function (deferred) {
            // get the processor data
            var processorData = source.datum();

            // populate the source processor information
            $('#processor-source').show();
            $('#processor-source-name').text(processorData.component.name);
            $('#processor-source-type').text(nf.Common.substringAfterLast(processorData.component.type, '.'));

            // populate the connection source details
            $('#connection-source-id').val(processorData.id);
            $('#connection-source-component-id').val(processorData.id);

            // populate the group details
            $('#connection-source-group-id').val(nf.Canvas.getGroupId());
            $('#connection-source-group-name').text(nf.Canvas.getGroupName());

            // show the available relationships
            $('#relationship-names-container').show();

            deferred.resolve(processorData.component);
        });
    };

    /**
     * Initializes the source when the source is a process group.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceProcessGroup = function (source) {
        return $.Deferred(function (deferred) {
            // get the process group data
            var processGroupData = source.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(processGroupData.id),
                data: {
                    verbose: true
                },
                dataType: 'json'
            }).done(function (response) {
                var processGroup = response.processGroupFlow;
                var processGroupContents = processGroup.flow;

                // show the output port options
                var options = [];
                $.each(processGroupContents.outputPorts, function (i, outputPort) {
                    if (outputPort.permissions.canRead && outputPort.permissions.canWrite) {
                        var component = outputPort.component;
                        options.push({
                            text: component.name,
                            value: component.id,
                            description: nf.Common.escapeHtml(component.comments)
                        });
                    }
                });

                // only proceed if there are output ports
                if (!nf.Common.isEmpty(options)) {
                    $('#output-port-source').show();

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#output-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-source-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-source-component-id').val(processGroup.id);

                    // populate the group details
                    $('#connection-source-group-id').val(processGroup.id);
                    $('#connection-source-group-name').text(processGroup.name);

                    deferred.resolve();
                } else {
                    // there are no output ports for this process group
                    nf.Dialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: '\'' + nf.Common.escapeHtml(processGroup.name) + '\' does not have any output ports.'
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nf.Common.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the source when the source is a remote process group.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceRemoteProcessGroup = function (source) {
        return $.Deferred(function (deferred) {
            // get the remote process group data
            var remoteProcessGroupData = source.datum();

            $.ajax({
                type: 'GET',
                url: remoteProcessGroupData.uri,
                data: {
                    verbose: true
                },
                dataType: 'json'
            }).done(function (response) {
                var remoteProcessGroup = response.component;
                var remoteProcessGroupContents = remoteProcessGroup.contents;

                // only proceed if there are output ports
                if (!nf.Common.isEmpty(remoteProcessGroupContents.outputPorts)) {
                    $('#output-port-source').show();

                    // show the output port options
                    var options = [];
                    $.each(remoteProcessGroupContents.outputPorts, function (i, outputPort) {
                        options.push({
                            text: outputPort.name,
                            value: outputPort.id,
                            disabled: outputPort.exists === false,
                            description: nf.Common.escapeHtml(outputPort.comments)
                        });
                    });

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#output-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-source-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-source-component-id').val(remoteProcessGroup.id);

                    // populate the group details
                    $('#connection-source-group-id').val(remoteProcessGroup.id);
                    $('#connection-source-group-name').text(remoteProcessGroup.name);

                    deferred.resolve();
                } else {
                    // there are no relationships for this processor
                    nf.Dialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: '\'' + nf.Common.escapeHtml(remoteProcessGroup.name) + '\' does not have any output ports.'
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nf.Common.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    var initializeDestinationNewConnectionDialog = function (destination) {
        if (nf.CanvasUtils.isOutputPort(destination)) {
            return initializeDestinationOutputPort(destination);
        } else if (nf.CanvasUtils.isProcessor(destination)) {
            return initializeDestinationProcessor(destination);
        } else if (nf.CanvasUtils.isRemoteProcessGroup(destination)) {
            return initializeDestinationRemoteProcessGroup(destination);
        } else if (nf.CanvasUtils.isFunnel(destination)) {
            return initializeDestinationFunnel(destination);
        } else {
            return initializeDestinationProcessGroup(destination);
        }
    };

    var initializeDestinationOutputPort = function (destination) {
        return $.Deferred(function (deferred) {
            var outputPortData = destination.datum();

            $('#output-port-destination').show();
            $('#output-port-destination-name').text(outputPortData.component.name);

            // populate the connection destination details
            $('#connection-destination-id').val(outputPortData.id);
            $('#connection-destination-component-id').val(outputPortData.id);

            // populate the group details
            $('#connection-destination-group-id').val(nf.Canvas.getGroupId());
            $('#connection-destination-group-name').text(nf.Canvas.getGroupName());

            deferred.resolve();
        }).promise();
    };

    var initializeDestinationFunnel = function (destination) {
        return $.Deferred(function (deferred) {
            var funnelData = destination.datum();

            $('#funnel-destination').show();

            // populate the connection destination details
            $('#connection-destination-id').val(funnelData.id);
            $('#connection-destination-component-id').val(funnelData.id);

            // populate the group details
            $('#connection-destination-group-id').val(nf.Canvas.getGroupId());
            $('#connection-destination-group-name').text(nf.Canvas.getGroupName());

            deferred.resolve();
        }).promise();
    };

    var initializeDestinationProcessor = function (destination) {
        return $.Deferred(function (deferred) {
            var processorData = destination.datum();

            $('#processor-destination').show();
            $('#processor-destination-name').text(processorData.component.name);
            $('#processor-destination-type').text(nf.Common.substringAfterLast(processorData.component.type, '.'));

            // populate the connection destination details
            $('#connection-destination-id').val(processorData.id);
            $('#connection-destination-component-id').val(processorData.id);

            // populate the group details
            $('#connection-destination-group-id').val(nf.Canvas.getGroupId());
            $('#connection-destination-group-name').text(nf.Canvas.getGroupName());

            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the destination when the destination is a process group.
     *
     * @argument {selection} destination        The destination
     */
    var initializeDestinationProcessGroup = function (destination) {
        return $.Deferred(function (deferred) {
            var processGroupData = destination.datum();

            $.ajax({
                type: 'GET',
                url: config.urls.api + '/flow/process-groups/' + encodeURIComponent(processGroupData.id),
                dataType: 'json'
            }).done(function (response) {
                var processGroup = response.processGroupFlow;
                var processGroupContents = processGroup.flow;

                // show the input port options
                var options = [];
                $.each(processGroupContents.inputPorts, function (i, inputPort) {
                    if (inputPort.permissions.canRead && inputPort.permissions.canWrite) {
                        var component = inputPort.component;
                        options.push({
                            text: component.name,
                            value: component.id,
                            description: nf.Common.escapeHtml(component.comments)
                        });
                    }
                });

                // only proceed if there are output ports
                if (!nf.Common.isEmpty(options)) {
                    $('#input-port-destination').show();

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#input-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-destination-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-destination-component-id').val(processGroup.id);

                    // populate the group details
                    $('#connection-destination-group-id').val(processGroup.id);
                    $('#connection-destination-group-name').text(processGroup.name);

                    deferred.resolve();
                } else {
                    // there are no relationships for this processor
                    nf.Dialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: '\'' + nf.Common.escapeHtml(processGroup.name) + '\' does not have any input ports.'
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nf.Common.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the source when the source is a remote process group.
     *
     * @argument {selection} destination        The destination
     */
    var initializeDestinationRemoteProcessGroup = function (destination) {
        return $.Deferred(function (deferred) {
            var remoteProcessGroupData = destination.datum();

            $.ajax({
                type: 'GET',
                url: remoteProcessGroupData.uri,
                data: {
                    verbose: true
                },
                dataType: 'json'
            }).done(function (response) {
                var remoteProcessGroup = response.component;
                var remoteProcessGroupContents = remoteProcessGroup.contents;

                // only proceed if there are output ports
                if (!nf.Common.isEmpty(remoteProcessGroupContents.inputPorts)) {
                    $('#input-port-destination').show();

                    // show the input port options
                    var options = [];
                    $.each(remoteProcessGroupContents.inputPorts, function (i, inputPort) {
                        options.push({
                            text: inputPort.name,
                            value: inputPort.id,
                            disabled: inputPort.exists === false,
                            description: nf.Common.escapeHtml(inputPort.comments)
                        });
                    });

                    // sort the options
                    options.sort(function (a, b) {
                        return a.text.localeCompare(b.text);
                    });

                    // create the combo
                    $('#input-port-options').combo({
                        options: options,
                        maxHeight: 300,
                        select: function (option) {
                            $('#connection-destination-id').val(option.value);
                        }
                    });

                    // populate the connection details
                    $('#connection-destination-component-id').val(remoteProcessGroup.id);

                    // populate the group details
                    $('#connection-destination-group-id').val(remoteProcessGroup.id);
                    $('#connection-destination-group-name').text(remoteProcessGroup.name);

                    deferred.resolve();
                } else {
                    // there are no relationships for this processor
                    nf.Dialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: '\'' + nf.Common.escapeHtml(remoteProcessGroup.name) + '\' does not have any input ports.'
                    });

                    // reset the dialog
                    resetDialog();

                    deferred.reject();
                }
            }).fail(function (xhr, status, error) {
                // handle the error
                nf.Common.handleAjaxError(xhr, status, error);

                deferred.reject();
            });
        }).promise();
    };

    /**
     * Initializes the source panel for groups.
     *
     * @argument {selection} source    The source of the connection
     */
    var initializeSourceReadOnlyGroup = function (source) {
        return $.Deferred(function (deferred) {
            var sourceData = source.datum();

            // populate the port information
            $('#read-only-output-port-source').show();

            // populate the component information
            $('#connection-source-component-id').val(sourceData.id);

            // populate the group details
            $('#connection-source-group-id').val(sourceData.id);
            $('#connection-source-group-name').text(sourceData.component.name);

            // resolve the deferred
            deferred.resolve();
        }).promise();
    };

    /**
     * Initializes the source in the existing connection dialog.
     *
     * @argument {selection} source        The source
     */
    var initializeSourceEditConnectionDialog = function (source) {
        if (nf.CanvasUtils.isProcessor(source)) {
            return initializeSourceProcessor(source);
        } else if (nf.CanvasUtils.isInputPort(source)) {
            return initializeSourceInputPort(source);
        } else if (nf.CanvasUtils.isFunnel(source)) {
            return initializeSourceFunnel(source);
        } else {
            return initializeSourceReadOnlyGroup(source);
        }
    };

    /**
     * Initializes the destination in the existing connection dialog.
     *
     * @argument {selection} destination        The destination
     */
    var initializeDestinationEditConnectionDialog = function (destination) {
        if (nf.CanvasUtils.isProcessor(destination)) {
            return initializeDestinationProcessor(destination);
        } else if (nf.CanvasUtils.isOutputPort(destination)) {
            return initializeDestinationOutputPort(destination);
        } else if (nf.CanvasUtils.isRemoteProcessGroup(destination)) {
            return initializeDestinationRemoteProcessGroup(destination);
        } else if (nf.CanvasUtils.isFunnel(destination)) {
            return initializeDestinationFunnel(destination);
        } else {
            return initializeDestinationProcessGroup(destination);
        }
    };

    /**
     * Creates an option for the specified relationship name.
     *
     * @argument {string} name      The relationship name
     */
    var createRelationshipOption = function (name) {
        var relationshipLabel = $('<div class="relationship-name ellipsis"></div>').text(name);
        var relationshipValue = $('<span class="relationship-name-value hidden"></span>').text(name);
        return $('<div class="available-relationship-container"><div class="available-relationship nf-checkbox checkbox-unchecked"></div>' +
            '</div>').append(relationshipLabel).append(relationshipValue).appendTo('#relationship-names');
    };

    /**
     * Adds a new connection.
     *
     * @argument {array} selectedRelationships      The selected relationships
     */
    var addConnection = function (selectedRelationships) {
        // get the connection details
        var sourceId = $('#connection-source-id').val();
        var destinationId = $('#connection-destination-id').val();

        // get the selection components
        var sourceComponentId = $('#connection-source-component-id').val();
        var source = d3.select('#id-' + sourceComponentId);
        var destinationComponentId = $('#connection-destination-component-id').val();
        var destination = d3.select('#id-' + destinationComponentId);

        // get the source/destination data
        var sourceData = source.datum();
        var destinationData = destination.datum();

        // add bend points if we're dealing with a self loop
        var bends = [];
        if (sourceComponentId === destinationComponentId) {
            var rightCenter = {
                x: sourceData.position.x + (sourceData.dimensions.width),
                y: sourceData.position.y + (sourceData.dimensions.height / 2)
            };

            var xOffset = nf.Connection.config.selfLoopXOffset;
            var yOffset = nf.Connection.config.selfLoopYOffset;
            bends.push({
                'x': (rightCenter.x + xOffset),
                'y': (rightCenter.y - yOffset)
            });
            bends.push({
                'x': (rightCenter.x + xOffset),
                'y': (rightCenter.y + yOffset)
            });
        } else {
            var existingConnections = [];

            // get all connections for the source component
            var connectionsForSourceComponent = nf.Connection.getComponentConnections(sourceComponentId);
            $.each(connectionsForSourceComponent, function (_, connectionForSourceComponent) {
                // get the id for the source/destination component
                var connectionSourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(connectionForSourceComponent);
                var connectionDestinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(connectionForSourceComponent);

                // if the connection is between these same components, consider it for collisions
                if ((connectionSourceComponentId === sourceComponentId && connectionDestinationComponentId === destinationComponentId) ||
                    (connectionDestinationComponentId === sourceComponentId && connectionSourceComponentId === destinationComponentId)) {

                    // record all connections between these two components in question
                    existingConnections.push(connectionForSourceComponent);
                }
            });

            // if there are existing connections between these components, ensure the new connection won't collide
            if (existingConnections.length > 0) {
                var avoidCollision = false;
                $.each(existingConnections, function (_, existingConnection) {
                    // only consider multiple connections with no bend points a collision, the existance of 
                    // bend points suggests that the user has placed the connection into a desired location
                    if (nf.Common.isEmpty(existingConnection.bends)) {
                        avoidCollision = true;
                        return false;
                    }
                });

                // if we need to avoid a collision
                if (avoidCollision === true) {
                    // determine the middle of the source/destination components
                    var sourceMiddle = [sourceData.position.x + (sourceData.dimensions.width / 2), sourceData.position.y + (sourceData.dimensions.height / 2)];
                    var destinationMiddle = [destinationData.position.x + (destinationData.dimensions.width / 2), destinationData.position.y + (destinationData.dimensions.height / 2)];

                    // detect if the line is more horizontal or vertical
                    var slope = ((sourceMiddle[1] - destinationMiddle[1]) / (sourceMiddle[0] - destinationMiddle[0]));
                    var isMoreHorizontal = slope <= 1 && slope >= -1;

                    // determines if the specified coordinate collides with another connection
                    var collides = function (x, y) {
                        var collides = false;
                        $.each(existingConnections, function (_, existingConnection) {
                            if (!nf.Common.isEmpty(existingConnection.bends)) {
                                if (isMoreHorizontal) {
                                    // horizontal lines are adjusted in the y space
                                    if (existingConnection.bends[0].y === y) {
                                        collides = true;
                                        return false;
                                    }
                                } else {
                                    // vertical lines are adjusted in the x space
                                    if (existingConnection.bends[0].x === x) {
                                        collides = true;
                                        return false;
                                    }
                                }
                            }
                        });
                        return collides;
                    };

                    // find the mid point on the connection
                    var xCandidate = (sourceMiddle[0] + destinationMiddle[0]) / 2;
                    var yCandidate = (sourceMiddle[1] + destinationMiddle[1]) / 2;

                    // attempt to position this connection so it doesn't collide
                    var xStep = isMoreHorizontal ? 0 : CONNECTION_OFFSET_X_INCREMENT;
                    var yStep = isMoreHorizontal ? CONNECTION_OFFSET_Y_INCREMENT : 0;
                    var positioned = false;
                    while (positioned === false) {
                        // consider above and below, then increment and try again (if necessary)
                        if (collides(xCandidate - xStep, yCandidate - yStep) === false) {
                            bends.push({
                                'x': (xCandidate - xStep),
                                'y': (yCandidate - yStep)
                            });
                            positioned = true;
                        } else if (collides(xCandidate + xStep, yCandidate + yStep) === false) {
                            bends.push({
                                'x': (xCandidate + xStep),
                                'y': (yCandidate + yStep)
                            });
                            positioned = true;
                        }

                        if (isMoreHorizontal) {
                            yStep += CONNECTION_OFFSET_Y_INCREMENT;
                        } else {
                            xStep += CONNECTION_OFFSET_X_INCREMENT;
                        }
                    }
                }
            }
        }

        // determine the source group id
        var sourceGroupId = $('#connection-source-group-id').val();
        var destinationGroupId = $('#connection-destination-group-id').val();

        // determine the source and destination types
        var sourceType = nf.CanvasUtils.getConnectableTypeForSource(source);
        var destinationType = nf.CanvasUtils.getConnectableTypeForDestination(destination);

        // get the settings
        var connectionName = $('#connection-name').val();
        var flowFileExpiration = $('#flow-file-expiration').val();
        var backPressureObjectThreshold = $('#back-pressure-object-threshold').val();
        var backPressureDataSizeThreshold = $('#back-pressure-data-size-threshold').val();
        var prioritizers = $('#prioritizer-selected').sortable('toArray');

        if (validateSettings()) {
            var connectionEntity = {
                'revision': nf.Client.getRevision({
                    'revision': {
                        'version': 0
                    }
                }),
                'component': {
                    'name': connectionName,
                    'source': {
                        'id': sourceId,
                        'groupId': sourceGroupId,
                        'type': sourceType
                    },
                    'destination': {
                        'id': destinationId,
                        'groupId': destinationGroupId,
                        'type': destinationType
                    },
                    'selectedRelationships': selectedRelationships,
                    'flowFileExpiration': flowFileExpiration,
                    'backPressureDataSizeThreshold': backPressureDataSizeThreshold,
                    'backPressureObjectThreshold': backPressureObjectThreshold,
                    'bends': bends,
                    'prioritizers': prioritizers
                }
            };

            // create the new connection
            $.ajax({
                type: 'POST',
                url: config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/connections',
                data: JSON.stringify(connectionEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // add the connection
                nf.Graph.add({
                    'connections': [response]
                }, {
                    'selectAll': true
                });

                // reload the connections source/destination components
                nf.CanvasUtils.reloadConnectionSourceAndDestination(sourceComponentId, destinationComponentId);

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }).fail(function (xhr, status, error) {
                // handle the error
                nf.Common.handleAjaxError(xhr, status, error);
            });
        }
    };

    /**
     * Updates an existing connection.
     *
     * @argument {array} selectedRelationships          The selected relationships
     */
    var updateConnection = function (selectedRelationships) {
        // get the connection details
        var connectionId = $('#connection-id').text();
        var connectionUri = $('#connection-uri').val();

        // get the source details
        var sourceComponentId = $('#connection-source-component-id').val();

        // get the destination details
        var destinationComponentId = $('#connection-destination-component-id').val();
        var destination = d3.select('#id-' + destinationComponentId);
        var destinationType = nf.CanvasUtils.getConnectableTypeForDestination(destination);

        // get the destination details
        var destinationId = $('#connection-destination-id').val();
        var destinationGroupId = $('#connection-destination-group-id').val();

        // get the settings
        var connectionName = $('#connection-name').val();
        var flowFileExpiration = $('#flow-file-expiration').val();
        var backPressureObjectThreshold = $('#back-pressure-object-threshold').val();
        var backPressureDataSizeThreshold = $('#back-pressure-data-size-threshold').val();
        var prioritizers = $('#prioritizer-selected').sortable('toArray');

        if (validateSettings()) {
            var d = nf.Connection.get(connectionId);
            var connectionEntity = {
                'revision': nf.Client.getRevision(d),
                'component': {
                    'id': connectionId,
                    'name': connectionName,
                    'destination': {
                        'id': destinationId,
                        'groupId': destinationGroupId,
                        'type': destinationType
                    },
                    'selectedRelationships': selectedRelationships,
                    'flowFileExpiration': flowFileExpiration,
                    'backPressureDataSizeThreshold': backPressureDataSizeThreshold,
                    'backPressureObjectThreshold': backPressureObjectThreshold,
                    'prioritizers': prioritizers
                }
            };

            // update the connection
            return $.ajax({
                type: 'PUT',
                url: connectionUri,
                data: JSON.stringify(connectionEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                if (nf.Common.isDefinedAndNotNull(response.component)) {
                    // update this connection
                    nf.Connection.set(response);

                    // reload the connections source/destination components
                    nf.CanvasUtils.reloadConnectionSourceAndDestination(sourceComponentId, destinationComponentId);
                }
            }).fail(function (xhr, status, error) {
                if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                    nf.Dialog.showOkDialog({
                        headerText: 'Connection Configuration',
                        dialogContent: nf.Common.escapeHtml(xhr.responseText),
                    });
                } else {
                    nf.Common.handleAjaxError(xhr, status, error);
                }
            });
        } else {
            return $.Deferred(function (deferred) {
                deferred.reject();
            }).promise();
        }
    };

    /**
     * Returns an array of selected relationship names.
     */
    var getSelectedRelationships = function () {
        // get all available relationships
        var availableRelationships = $('#relationship-names');
        var selectedRelationships = [];

        // go through each relationship to determine which are selected
        $.each(availableRelationships.children(), function (i, relationshipElement) {
            var relationship = $(relationshipElement);

            // get each relationship and its corresponding checkbox
            var relationshipCheck = relationship.children('div.available-relationship');

            // see if this relationship has been selected
            if (relationshipCheck.hasClass('checkbox-checked')) {
                selectedRelationships.push(relationship.children('span.relationship-name-value').text());
            }
        });

        return selectedRelationships;
    };

    /**
     * Validates the specified settings.
     */
    var validateSettings = function () {
        var errors = [];

        // validate the settings
        if (nf.Common.isBlank($('#flow-file-expiration').val())) {
            errors.push('File expiration must be specified');
        }
        if (!$.isNumeric($('#back-pressure-object-threshold').val())) {
            errors.push('Back pressure object threshold must be an integer value');
        }
        if (nf.Common.isBlank($('#back-pressure-data-size-threshold').val())) {
            errors.push('Back pressure data size threshold must be specified');
        }

        if (errors.length > 0) {
            nf.Dialog.showOkDialog({
                headerText: 'Connection Configuration',
                dialogContent: nf.Common.formatUnorderedList(errors)
            });
            return false;
        } else {
            return true;
        }
    };

    /**
     * Resets the dialog.
     */
    var resetDialog = function () {
        // reset the prioritizers
        var selectedList = $('#prioritizer-selected');
        var availableList = $('#prioritizer-available');
        selectedList.children().detach().appendTo(availableList);

        // sort the available list
        var listItems = availableList.children('li').get();
        listItems.sort(function (a, b) {
            var compA = $(a).text().toUpperCase();
            var compB = $(b).text().toUpperCase();
            return (compA < compB) ? -1 : (compA > compB) ? 1 : 0;
        });

        // clear the available list and re-insert each list item
        $.each(listItems, function () {
            $(this).detach();
        });
        $.each(listItems, function () {
            $(this).appendTo(availableList);
        });

        // reset the fields
        $('#connection-name').val('');
        $('#relationship-names').css('border-width', '0').empty();
        $('#relationship-names-container').hide();

        // clear the id field
        nf.Common.clearField('connection-id');

        // hide all the connection source panels
        $('#processor-source').hide();
        $('#input-port-source').hide();
        $('#output-port-source').hide();
        $('#read-only-output-port-source').hide();
        $('#funnel-source').hide();

        // hide all the connection destination panels
        $('#processor-destination').hide();
        $('#input-port-destination').hide();
        $('#output-port-destination').hide();
        $('#funnel-destination').hide();

        // clear and destination details
        $('#connection-source-id').val('');
        $('#connection-source-component-id').val('');
        $('#connection-source-group-id').val('');

        // clear any destination details
        $('#connection-destination-id').val('');
        $('#connection-destination-component-id').val('');
        $('#connection-destination-group-id').val('');

        // clear any ports
        $('#output-port-options').empty();
        $('#input-port-options').empty();

        // see if the temp edge needs to be removed
        removeTempEdge();
    };

    return {
        init: function () {
            // initially hide the relationship names container
            $('#relationship-names-container').hide();

            // initialize the configure connection dialog
            $('#connection-configuration').modal({
                scrollableContentStyle: 'scrollable',
                headerText: 'Configure Connection',
                handler: {
                    close: function () {
                        // reset the dialog on close
                        resetDialog();
                    },
                    open: function () {
                        nf.Common.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });

            // initialize the properties tabs
            $('#connection-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'Details',
                    tabContentId: 'connection-details-tab-content'
                }, {
                    name: 'Settings',
                    tabContentId: 'connection-settings-tab-content'
                }]
            });

            // load the processor prioritizers
            $.ajax({
                type: 'GET',
                url: config.urls.prioritizers,
                dataType: 'json'
            }).done(function (response) {
                // create an element for each available prioritizer
                $.each(response.prioritizerTypes, function (i, documentedType) {
                    nf.ConnectionConfiguration.addAvailablePrioritizer('#prioritizer-available', documentedType);
                });

                // make the prioritizer containers sortable
                $('#prioritizer-available, #prioritizer-selected').sortable({
                    connectWith: 'ul',
                    placeholder: 'ui-state-highlight',
                    scroll: true,
                    opacity: 0.6
                });
                $('#prioritizer-available, #prioritizer-selected').disableSelection();
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Adds the specified prioritizer to the specified container.
         *
         * @argument {string} prioritizerContainer      The dom Id of the prioritizer container
         * @argument {object} prioritizerType           The type of prioritizer
         */
        addAvailablePrioritizer: function (prioritizerContainer, prioritizerType) {
            var type = prioritizerType.type;
            var name = nf.Common.substringAfterLast(type, '.');

            // add the prioritizers to the available list
            var prioritizerList = $(prioritizerContainer);
            var prioritizer = $('<li></li>').append($('<span style="float: left;"></span>').text(name)).attr('id', type).addClass('ui-state-default').appendTo(prioritizerList);

            // add the description if applicable
            if (nf.Common.isDefinedAndNotNull(prioritizerType.description)) {
                $('<div class="fa fa-question-circle" style="float: right; margin-right: 5px;""></div>').appendTo(prioritizer).qtip($.extend({
                    content: nf.Common.escapeHtml(prioritizerType.description)
                }, nf.Common.config.tooltipConfig));
            }
        },

        /**
         * Shows the dialog for creating a new connection.
         *
         * @argument {string} sourceId      The source id
         * @argument {string} destinationId The destination id
         */
        createConnection: function (sourceId, destinationId) {
            // select the source and destination
            var source = d3.select('#id-' + sourceId);
            var destination = d3.select('#id-' + destinationId);

            if (source.empty() || destination.empty()) {
                return;
            }

            // initialize the connection dialog
            $.when(initializeSourceNewConnectionDialog(source), initializeDestinationNewConnectionDialog(destination)).done(function () {
                // set the default values
                $('#flow-file-expiration').val('0 sec');
                $('#back-pressure-object-threshold').val('10000');
                $('#back-pressure-data-size-threshold').val('1 GB');

                // select the first tab
                $('#connection-configuration-tabs').find('li:first').click();

                // configure the header and show the dialog
                $('#connection-configuration').modal('setHeaderText', 'Create Connection').modal('show');

                // add the ellipsis if necessary
                $('#connection-configuration div.relationship-name').ellipsis();

                // fill in the connection id
                nf.Common.populateField('connection-id', null);

                // show the border if necessary
                var relationshipNames = $('#relationship-names');
                if (relationshipNames.is(':visible') && relationshipNames.get(0).scrollHeight > relationshipNames.innerHeight()) {
                    relationshipNames.css('border-width', '1px');
                }
            }).fail(function () {
                // see if the temp edge needs to be removed
                removeTempEdge();
            });
        },

        /**
         * Shows the configuration for the specified connection. If a destination is
         * specified it will be considered a new destination.
         *
         * @argument {selection} selection         The connection entry
         * @argument {selection} destination          Optional new destination
         */
        showConfiguration: function (selection, destination) {
            return $.Deferred(function (deferred) {
                var connectionEntry = selection.datum();
                var connection = connectionEntry.component;

                // identify the source component
                var sourceComponentId = nf.CanvasUtils.getConnectionSourceComponentId(connectionEntry);
                var source = d3.select('#id-' + sourceComponentId);

                // identify the destination component
                if (nf.Common.isUndefinedOrNull(destination)) {
                    var destinationComponentId = nf.CanvasUtils.getConnectionDestinationComponentId(connectionEntry);
                    destination = d3.select('#id-' + destinationComponentId);
                }

                // initialize the connection dialog
                $.when(initializeSourceEditConnectionDialog(source), initializeDestinationEditConnectionDialog(destination)).done(function () {
                    var availableRelationships = connection.availableRelationships;
                    var selectedRelationships = connection.selectedRelationships;

                    // show the available relationship if applicable
                    if (nf.Common.isDefinedAndNotNull(availableRelationships) || nf.Common.isDefinedAndNotNull(selectedRelationships)) {
                        // populate the available connections
                        $.each(availableRelationships, function (i, name) {
                            createRelationshipOption(name);
                        });

                        // ensure all selected relationships are present
                        // (may be undefined) and selected
                        $.each(selectedRelationships, function (i, name) {
                            // mark undefined relationships accordingly
                            if ($.inArray(name, availableRelationships) === -1) {
                                var option = createRelationshipOption(name);
                                $(option).children('div.relationship-name').addClass('undefined');
                            }

                            // ensure all selected relationships are checked
                            var relationships = $('#relationship-names').children('div');
                            $.each(relationships, function (i, relationship) {
                                var relationshipName = $(relationship).children('span.relationship-name-value');
                                if (relationshipName.text() === name) {
                                    $(relationship).children('div.available-relationship').removeClass('checkbox-unchecked').addClass('checkbox-checked');
                                }
                            });
                        });
                    }

                    // if the source is a process group or remote process group, select the appropriate port if applicable
                    if (nf.CanvasUtils.isProcessGroup(source) || nf.CanvasUtils.isRemoteProcessGroup(source)) {
                        // populate the connection source details
                        $('#connection-source-id').val(connection.source.id);
                        $('#read-only-output-port-name').text(connection.source.name);
                    }

                    // if the destination is a process gorup or remote process group, select the appropriate port if applicable
                    if (nf.CanvasUtils.isProcessGroup(destination) || nf.CanvasUtils.isRemoteProcessGroup(destination)) {
                        var destinationData = destination.datum();

                        // when the group ids differ, its a new destination component so we don't want to preselect any port
                        if (connection.destination.groupId === destinationData.id) {
                            $('#input-port-options').combo('setSelectedOption', {
                                value: connection.destination.id
                            });
                        }
                    }

                    // set the connection settings
                    $('#connection-name').val(connection.name);
                    $('#flow-file-expiration').val(connection.flowFileExpiration);
                    $('#back-pressure-object-threshold').val(connection.backPressureObjectThreshold);
                    $('#back-pressure-data-size-threshold').val(connection.backPressureDataSizeThreshold);

                    // format the connection id
                    nf.Common.populateField('connection-id', connection.id);

                    // handle each prioritizer
                    $.each(connection.prioritizers, function (i, type) {
                        $('#prioritizer-available').children('li[id="' + type + '"]').detach().appendTo('#prioritizer-selected');
                    });

                    // store the connection details
                    $('#connection-uri').val(connectionEntry.uri);

                    // configure the button model
                    $('#connection-configuration').modal('setButtonModel', [{
                        buttonText: 'Apply',
                        color: {
                            base: '#728E9B',
                            hover: '#004849',
                            text: '#ffffff'
                        },
                        handler: {
                            click: function () {
                                // get the selected relationships
                                var selectedRelationships = getSelectedRelationships();

                                // see if we're working with a processor as the source
                                if (nf.CanvasUtils.isProcessor(source)) {
                                    if (selectedRelationships.length > 0) {
                                        // if there are relationships selected update
                                        updateConnection(selectedRelationships).done(function () {
                                            deferred.resolve();
                                        }).fail(function () {
                                            deferred.reject();
                                        });
                                    } else {
                                        // inform users that no relationships were selected and the source is a processor
                                        nf.Dialog.showOkDialog({
                                            headerText: 'Connection Configuration',
                                            dialogContent: 'The connection must have at least one relationship selected.'
                                        });

                                        // reject the deferred
                                        deferred.reject();
                                    }
                                } else {
                                    // there are no relationships, but the source wasn't a processor, so update anyway
                                    updateConnection(undefined).done(function () {
                                        deferred.resolve();
                                    }).fail(function () {
                                        deferred.reject();
                                    });
                                }

                                // close the dialog
                                $('#connection-configuration').modal('hide');
                            }
                        }
                    },
                        {
                            buttonText: 'Cancel',
                            color: {
                                base: '#E3E8EB',
                                hover: '#C7D2D7',
                                text: '#004849'
                            },
                            handler: {
                                click: function () {
                                    // hide the dialog
                                    $('#connection-configuration').modal('hide');

                                    // reject the deferred
                                    deferred.reject();
                                }
                            }
                        }]);

                    // select the first tab
                    $('#connection-configuration-tabs').find('li:first').click();

                    // show the details dialog
                    $('#connection-configuration').modal('setHeaderText', 'Configure Connection').modal('show');

                    // add the ellipsis if necessary
                    $('#connection-configuration div.relationship-name').ellipsis();

                    // show the border if necessary
                    var relationshipNames = $('#relationship-names');
                    if (relationshipNames.is(':visible') && relationshipNames.get(0).scrollHeight > relationshipNames.innerHeight()) {
                        relationshipNames.css('border-width', '1px');
                    }
                }).fail(function () {
                    deferred.reject();
                });
            }).promise();
        }
    };
}());