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

nf.ng.TemplateComponent = function (serviceProvider) {
    'use strict';

    /**
     * Instantiates the specified template.
     *
     * @argument {string} templateId        The template id.
     * @argument {object} pt                The point that the template was dropped.
     */
    var createTemplate = function (templateId, pt) {
        var instantiateTemplateInstance = {
            'templateId': templateId,
            'originX': pt.x,
            'originY': pt.y
        };

        // create a new instance of the new template
        $.ajax({
            type: 'POST',
            url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/template-instance',
            data: JSON.stringify(instantiateTemplateInstance),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            // populate the graph accordingly
            nf.Graph.add(response.flow, {
                'selectAll': true
            });

            // update component visibility
            nf.Canvas.View.updateVisibility();

            // update the birdseye
            nf.Birdseye.refresh();
        }).fail(nf.Common.handleAjaxError);
    };

    function TemplateComponent() {

        this.icon = 'icon icon-template';

        this.hoverIcon = 'icon icon-template-add';

        /**
         * The template component's modal.
         */
        this.modal = {

            /**
             * Gets the modal element.
             *
             * @returns {*|jQuery|HTMLElement}
             */
            getElement: function () {
                return $('#instantiate-template-dialog');
            },

            /**
             * Initialize the modal.
             */
            init: function () {
                // configure the instantiate template dialog
                this.getElement().modal({
                    scrollableContentStyle: 'scrollable',
                    headerText: 'Add Template'
                });
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

    TemplateComponent.prototype = {
        constructor: TemplateComponent,

        /**
         * Gets the component.
         *
         * @returns {*|jQuery|HTMLElement}
         */
        getElement: function () {
            return $('#template-component');
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
            this.promptForTemplate(pt);
        },

        /**
         * The drag icon for the toolbox component.
         *
         * @param event
         * @returns {*|jQuery|HTMLElement}
         */
        dragIcon: function (event) {
            return $('<div class="icon icon-template-add"></div>');
        },

        /**
         * Prompts the user to select a template.
         *
         * @argument {object} pt        The point that the template was dropped.
         */
        promptForTemplate: function (pt) {
            var self = this;
            $.ajax({
                type: 'GET',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/flow/templates',
                dataType: 'json'
            }).done(function (response) {
                var templates = response.templates;
                if (nf.Common.isDefinedAndNotNull(templates) && templates.length > 0) {
                    var options = [];
                    $.each(templates, function (_, templateEntity) {
                        if (templateEntity.permissions.canRead === true) {
                            options.push({
                                text: templateEntity.template.name,
                                value: templateEntity.id,
                                description: nf.Common.escapeHtml(templateEntity.template.description)
                            });
                        }
                    });

                    // configure the templates combo
                    $('#available-templates').combo({
                        maxHeight: 300,
                        options: options
                    });

                    // update the button model
                    self.modal.update('setButtonModel', [{
                        buttonText: 'Add',
                        color: {
                            base: '#728E9B',
                            hover: '#004849',
                            text: '#ffffff'
                        },
                        handler: {
                            click: function () {
                                // get the type of processor currently selected
                                var selectedOption = $('#available-templates').combo('getSelectedOption');
                                var templateId = selectedOption.value;

                                // hide the dialog
                                self.modal.hide();

                                // instantiate the specified template
                                createTemplate(templateId, pt);
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
                                    self.modal.hide();
                                }
                            }
                        }]);

                    // show the dialog
                    self.modal.show();
                } else {
                    nf.Dialog.showOkDialog({
                        headerText: 'Instantiate Template',
                        dialogContent: 'No templates have been loaded into this NiFi.'
                    });
                }

            }).fail(nf.Common.handleAjaxError);
        }
    }

    var templateComponent = new TemplateComponent();
    return templateComponent;
};