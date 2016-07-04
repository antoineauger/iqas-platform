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

nf.ng.OutputPortComponent = function (serviceProvider) {
    'use strict';

    /**
     * Create the input port and add to the graph.
     *
     * @argument {string} portName          The output port name.
     * @argument {object} pt                The point that the output port was dropped.
     */
    var createOutputPort = function (portName, pt) {
        var outputPortEntity = {
            'revision': nf.Client.getRevision({
                'revision': {
                    'version': 0
                }
            }),
            'component': {
                'name': portName,
                'position': {
                    'x': pt.x,
                    'y': pt.y
                }
            }
        };

        // create a new processor of the defined type
        $.ajax({
            type: 'POST',
            url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/output-ports',
            data: JSON.stringify(outputPortEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            if (nf.Common.isDefinedAndNotNull(response.component)) {
                // add the port to the graph
                nf.Graph.add({
                    'outputPorts': [response]
                }, {
                    'selectAll': true
                });

                // update component visibility
                nf.Canvas.View.updateVisibility();

                // update the birdseye
                nf.Birdseye.refresh();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    function OutputPortComponent() {

        /**
         * The output port component's modal.
         */
        this.modal = {

            /**
             * Gets the modal element.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#new-port-dialog'); //Reuse the input port dialog....
            },

            /**
             * Initialize the modal.
             */
            init: function () {
                //Reuse the input port dialog....
            },

            /**
             * Updates the modal config.
             *
             * @param {string} name             The name of the property to update.
             * @param {object|array} config     The config for the `name`.
             */
            update: function (name, config) {
                this.getElement().modal(name, config);
            },

            /**
             * Show the modal.
             */
            show: function () {
                this.getElement().modal('show');
            },

            /**
             * Hide the modal.
             */
            hide: function () {
                this.getElement().modal('hide');
            }
        };
    }

    OutputPortComponent.prototype = {
        constructor: OutputPortComponent,

        /**
         * Gets the component.
         *
         * @returns {*|jQuery|HTMLElement}
         */
        getElement: function () {
            return $('#port-out-component');
        },

        /**
         * Enable the component.
         */
        enabled: function () {
            this.getElement().attr('disabled', false);
        },

        /**
         * Disable the component.
         */
        disabled: function () {
            this.getElement().attr('disabled', true);
        },

        /**
         * Handler function for when component is dropped on the canvas.
         *
         * @argument {object} pt        The point that the component was dropped.
         */
        dropHandler: function (pt) {
            this.promptForOutputPortName(pt);
        },

        /**
         * The drag icon for the toolbox component.
         *
         * @param event
         * @returns {*|jQuery|HTMLElement}
         */
        dragIcon: function (event) {
            return $('<div class="icon icon-port-out-add"></div>');
        },

        /**
         * Prompts the user to enter the name for the output port.
         *
         * @argument {object} pt        The point that the output port was dropped.
         */
        promptForOutputPortName: function (pt) {
            var self = this;
            var addOutputPort = function () {
                // get the name of the output port and clear the textfield
                var portName = $('#new-port-name').val();

                // hide the dialog
                self.modal.hide();

                // create the output port
                createOutputPort(portName, pt);
            };

            this.modal.update('setButtonModel', [{
                buttonText: 'Add',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: addOutputPort
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
                            self.modal.hide();
                        }
                    }
                }]);

            // update the port type
            $('#new-port-type').text('Output');

            // set the focus and show the dialog
            this.modal.show();

            // set up the focus and key handlers
            $('#new-port-name').focus().off('keyup').on('keyup', function (e) {
                var code = e.keyCode ? e.keyCode : e.which;
                if (code === $.ui.keyCode.ENTER) {
                    addOutputPort();
                }
            });
        }
    }

    var outputPortComponent = new OutputPortComponent();
    return outputPortComponent;
};