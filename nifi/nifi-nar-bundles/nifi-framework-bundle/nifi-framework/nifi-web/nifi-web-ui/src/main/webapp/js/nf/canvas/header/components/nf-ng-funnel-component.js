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

nf.ng.FunnelComponent = function (serviceProvider) {
    'use strict';

    function FunnelComponent() {
        this.icon = 'icon icon-funnel';

        this.hoverIcon = 'icon icon-funnel-add';
    }
    FunnelComponent.prototype = {
        constructor: FunnelComponent,

        /**
         * Gets the component.
         *
         * @returns {*|jQuery|HTMLElement}
         */
        getElement: function() {
            return $('#funnel-component');
        },

        /**
         * Enable the component.
         */
        enabled: function() {
            this.getElement().attr('disabled', false);
        },

        /**
         * Disable the component.
         */
        disabled: function() {
            this.getElement().attr('disabled', true);
        },

        /**
         * Handler function for when component is dropped on the canvas.
         *
         * @argument {object} pt        The point that the component was dropped.
         */
        dropHandler: function(pt) {
            this.createFunnel(pt);
        },

        /**
         * The drag icon for the toolbox component.
         *
         * @param event
         * @returns {*|jQuery|HTMLElement}
         */
        dragIcon: function (event) {
            return $('<div class="icon icon-funnel-add"></div>');
        },

        /**
         * Creates a new funnel at the specified point.
         *
         * @argument {object} pt        The point that the funnel was dropped.
         */
        createFunnel: function(pt) {
            var outputPortEntity = {
                'revision': nf.Client.getRevision({
                    'revision': {
                        'version': 0
                    }
                }),
                'component': {
                    'position': {
                        'x': pt.x,
                        'y': pt.y
                    }
                }
            };

            // create a new funnel
            $.ajax({
                type: 'POST',
                url: serviceProvider.headerCtrl.toolboxCtrl.config.urls.api + '/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/funnels',
                data: JSON.stringify(outputPortEntity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // add the funnel to the graph
                nf.Graph.add({
                    'funnels': [response]
                }, {
                    'selectAll': true
                });

                // update the birdseye
                nf.Birdseye.refresh();
            }).fail(nf.Common.handleAjaxError);
        }
    }

    var funnelComponent = new FunnelComponent();
    return funnelComponent;
};