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

nf.ng.Canvas.FlowStatusCtrl = function (serviceProvider, $sanitize) {
    'use strict';

    var config = {
        search: 'Search',
        urls: {
            search: '../nifi-api/flow/search-results',
            status: '../nifi-api/flow/status'
        }
    };

    function FlowStatusCtrl() {
        this.connectedNodesCount = "-";
        this.activeThreadCount = "-";
        this.totalQueued = "-";
        this.controllerTransmittingCount = "-";
        this.controllerNotTransmittingCount = "-";
        this.controllerRunningCount = "-";
        this.controllerStoppedCount = "-";
        this.controllerInvalidCount = "-";
        this.controllerDisabledCount = "-";
        this.statsLastRefreshed = "-";

        /**
         * The search controller.
         */
        this.search = {

            /**
             * Get the search input element.
             */
            getInputElement: function () {
                return $('#search-field');
            },

            /**
             * Get the search button element.
             */
            getButtonElement: function () {
                return $('#search-button');
            },

            /**
             * Get the search container element.
             */
            getSearchContainerElement: function () {
                return $('#search-container');
            },

            /**
             * Initialize the search controller.
             */
            init: function () {

                var self = this;

                // Create new jQuery UI widget
                $.widget('nf.searchAutocomplete', $.ui.autocomplete, {
                    reset: function () {
                        this.term = null;
                    },
                    _resizeMenu: function () {
                        var ul = this.menu.element;
                        ul.width(399);
                    },
                    _normalize: function (searchResults) {
                        var items = [];
                        items.push(searchResults);
                        return items;
                    },
                    _renderMenu: function (ul, items) {
                        var self = this;

                        // the object that holds the search results is normalized into a single element array
                        var searchResults = items[0];

                        // show all processors
                        if (!nf.Common.isEmpty(searchResults.processorResults)) {
                            ul.append('<li class="search-header"><div class="search-result-icon icon icon-processor"></div>Processors</li>');
                            $.each(searchResults.processorResults, function (i, processorMatch) {
                                self._renderItem(ul, processorMatch);
                            });
                        }

                        // show all process groups
                        if (!nf.Common.isEmpty(searchResults.processGroupResults)) {
                            ul.append('<li class="search-header"><div class="search-result-icon icon icon-group"></div>Process Groups</li>');
                            $.each(searchResults.processGroupResults, function (i, processGroupMatch) {
                                self._renderItem(ul, processGroupMatch);
                            });
                        }

                        // show all remote process groups
                        if (!nf.Common.isEmpty(searchResults.remoteProcessGroupResults)) {
                            ul.append('<li class="search-header"><div class="search-result-icon icon icon-group-remote"></div>Remote Process Groups</li>');
                            $.each(searchResults.remoteProcessGroupResults, function (i, remoteProcessGroupMatch) {
                                self._renderItem(ul, remoteProcessGroupMatch);
                            });
                        }

                        // show all connections
                        if (!nf.Common.isEmpty(searchResults.connectionResults)) {
                            ul.append('<li class="search-header"><div class="search-result-icon icon icon-connect"></div>Connections</li>');
                            $.each(searchResults.connectionResults, function (i, connectionMatch) {
                                self._renderItem(ul, connectionMatch);
                            });
                        }

                        // show all input ports
                        if (!nf.Common.isEmpty(searchResults.inputPortResults)) {
                            ul.append('<li class="search-header"><div class="search-result-icon icon icon-port-in"></div>Input Ports</li>');
                            $.each(searchResults.inputPortResults, function (i, inputPortMatch) {
                                self._renderItem(ul, inputPortMatch);
                            });
                        }

                        // show all output ports
                        if (!nf.Common.isEmpty(searchResults.outputPortResults)) {
                            ul.append('<li class="search-header"><div class="search-result-icon icon icon-port-out"></div>Output Ports</li>');
                            $.each(searchResults.outputPortResults, function (i, outputPortMatch) {
                                self._renderItem(ul, outputPortMatch);
                            });
                        }

                        // show all funnels
                        if (!nf.Common.isEmpty(searchResults.funnelResults)) {
                            ul.append('<li class="search-header"><div class="search-result-icon icon icon-funnel"></div>Funnels</li>');
                            $.each(searchResults.funnelResults, function (i, funnelMatch) {
                                self._renderItem(ul, funnelMatch);
                            });
                        }

                        // ensure there were some results
                        if (ul.children().length === 0) {
                            ul.append('<li class="unset search-no-matches">No results matched the search terms</li>');
                        }
                    },
                    _renderItem: function (ul, match) {
                        var itemContent = $('<a></a>').append($('<div class="search-match-header"></div>').text(match.name));
                        $.each(match.matches, function (i, match) {
                            itemContent.append($('<div class="search-match"></div>').text(match));
                        });
                        return $('<li></li>').data('ui-autocomplete-item', match).append(itemContent).appendTo(ul);
                    }
                })

                // configure the new searchAutocomplete jQuery UI widget
                this.getInputElement().searchAutocomplete({
                    appendTo: '#search-flow-results',
                    position: {
                        my: 'right top',
                        at: 'right bottom',
                        offset: '1 1'
                    },
                    source: function (request, response) {
                        // create the search request
                        $.ajax({
                            type: 'GET',
                            data: {
                                q: request.term
                            },
                            dataType: 'json',
                            url: config.urls.search
                        }).done(function (searchResponse) {
                            response(searchResponse.searchResultsDTO);
                        });
                    },
                    select: function (event, ui) {
                        var item = ui.item;

                        // show the selected component
                        nf.CanvasUtils.showComponent(item.groupId, item.id);

                        self.getInputElement().val('').blur();

                        // stop event propagation
                        return false;
                    },
                    open: function (event, ui) {
                        // show the glass pane
                        var searchField = $(this);
                        $('<div class="search-glass-pane"></div>').one('click', function () {
                        }).appendTo('body');
                    },
                    close: function (event, ui) {
                        // set the input text to '' and reset the cached term
                        $(this).searchAutocomplete('reset');
                        self.getInputElement().val('');

                        // remove the glass pane
                        $('div.search-glass-pane').remove();
                    }
                });

                // hide the search input
                self.toggleSearchField();
            },

            /**
             * Toggle/Slide the search field open/closed.
             */
            toggleSearchField: function () {
                var self = this;

                // hide the context menu if necessary
                nf.ContextMenu.hide();

                var isVisible = self.getInputElement().is(':visible');
                var display = 'none';
                var class1 = 'search-container-opened';
                var class2 = 'search-container-closed';
                if (!isVisible) {
                    self.getButtonElement().css('background-color', '#FFFFFF');
                    display = 'inline-block';
                    class1 = 'search-container-closed';
                    class2 = 'search-container-opened';
                } else {
                    self.getInputElement().css('display', display);
                }

                this.getSearchContainerElement().switchClass(class1, class2, 500, function () {
                    self.getInputElement().css('display', display);
                    if (!isVisible) {
                        self.getButtonElement().css('background-color', '#FFFFFF');
                        self.getInputElement().focus();
                    } else {
                        self.getButtonElement().css('background-color', '#E3E8EB');
                    }
                });
            }
        }

        /**
         * The bulletins controller.
         */
        this.bulletins = {

            /**
             * Update the bulletins.
             *
             * @param status  The controller status returned from the `../nifi-api/flow/status` endpoint.
             */
            update: function (status) {

                // icon for system bulletins
                var bulletinIcon = $('#bulletin-button');
                var currentBulletins = bulletinIcon.data('bulletins');

                // update the bulletins if necessary
                if (nf.Common.doBulletinsDiffer(currentBulletins, status.bulletins)) {
                    bulletinIcon.data('bulletins', status.bulletins);

                    // get the formatted the bulletins
                    var bulletins = nf.Common.getFormattedBulletins(status.bulletins);

                    // bulletins for this processor are now gone
                    if (bulletins.length === 0) {
                        if (bulletinIcon.data('qtip')) {
                            bulletinIcon.removeClass('has-bulletins').qtip('api').destroy(true);
                        }
                    } else {
                        var newBulletins = nf.Common.formatUnorderedList(bulletins);

                        // different bulletins, refresh
                        if (bulletinIcon.data('qtip')) {
                            bulletinIcon.qtip('option', 'content.text', newBulletins);
                        } else {
                            // no bulletins before, show icon and tips
                            bulletinIcon.addClass('has-bulletins').qtip($.extend({
                                content: newBulletins
                            }, nf.CanvasUtils.config.systemTooltipConfig, {
                                position: {
                                    at: 'bottom left',
                                    my: 'top right',
                                    adjust: {
                                        x: 4
                                    }
                                }
                            }));
                        }
                    }
                }

                // update controller service and reporting task bulletins
                nf.Settings.setBulletins(status.controllerServiceBulletins, status.reportingTaskBulletins);
            }

        }
    }

    FlowStatusCtrl.prototype = {
        constructor: FlowStatusCtrl,

        /**
         * Initialize the flow status controller.
         */
        init: function () {
            this.search.init();
        },

        /**
         * Reloads the current status of the flow.
         */
        reloadFlowStatus: function () {
            var self = this;

            return $.ajax({
                type: 'GET',
                url: config.urls.status,
                dataType: 'json'
            }).done(function (response) {
                // report the updated status
                if (nf.Common.isDefinedAndNotNull(response.controllerStatus)) {
                    self.update(response.controllerStatus);
                }
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Update the flow status counts.
         *
         * @param status  The controller status returned from the `../nifi-api/flow/status` endpoint.
         */
        update: function (status) {
            var controllerInvalidCountColor =
                (nf.Common.isDefinedAndNotNull(status.invalidCount) && (status.invalidCount > 0)) ?
                    '#BA554A' : '#728E9B';
            $('#controller-invalid-count').parent().css('color', controllerInvalidCountColor);

            if (nf.Common.isDefinedAndNotNull(status.connectedNodes)) {
                var connectedNodes = status.connectedNodes.split(' / ');
                var connectedNodesCountColor =
                    (connectedNodes.length === 2 && connectedNodes[0] !== connectedNodes[1]) ? '#BA554A' : '#728E9B';
                $('#connected-nodes-count').parent().css('color', connectedNodesCountColor);
            }

            // update the report values
            this.activeThreadCount = $sanitize(status.activeThreadCount);
            this.totalQueued = $sanitize(status.queued);

            // update the component counts
            this.controllerTransmittingCount =
                nf.Common.isDefinedAndNotNull(status.activeRemotePortCount) ?
                    $sanitize(status.activeRemotePortCount) : '-';

            this.controllerNotTransmittingCount =
                nf.Common.isDefinedAndNotNull(status.inactiveRemotePortCount) ?
                    $sanitize(status.inactiveRemotePortCount) : '-';

            this.controllerRunningCount =
                nf.Common.isDefinedAndNotNull(status.runningCount) ? $sanitize(status.runningCount) : '-';

            this.controllerStoppedCount =
                nf.Common.isDefinedAndNotNull(status.stoppedCount) ? $sanitize(status.stoppedCount) : '-';

            this.controllerInvalidCount =
                nf.Common.isDefinedAndNotNull(status.invalidCount) ? $sanitize(status.invalidCount) : '-';

            this.controllerDisabledCount =
                nf.Common.isDefinedAndNotNull(status.disabledCount) ? $sanitize(status.disabledCount) : '-';

            this.connectedNodesCount =
                nf.Common.isDefinedAndNotNull(status.connectedNodes) ? $sanitize(status.connectedNodes) : '-';

            this.bulletins.update(status);

            // handle any pending user request
            if (status.hasPendingAccounts === true) {
                $('#has-pending-accounts').show();
            } else {
                $('#has-pending-accounts').hide();
            }
        }
    }

    var flowStatusCtrl = new FlowStatusCtrl();
    return flowStatusCtrl;
};