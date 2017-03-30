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

nf.PolicyManagement = (function () {
    
    var config = {
        urls: {
            api: '../nifi-api',
            searchTenants: '../nifi-api/tenants/search-results'
        }
    };

    var initialized = false;

    var initAddTenantToPolicyDialog = function () {
        $('#new-policy-user-button').on('click', function () {
            $('#search-users-dialog').modal('show');
            $('#search-users-field').focus();
        });

        $('#delete-policy-button').on('click', function () {
            promptToDeletePolicy();
        });

        $('#search-users-dialog').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Add Users/Groups',
            buttons: [{
                buttonText: 'Add',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // add to table and update policy
                        var policyGrid = $('#policy-table').data('gridInstance');
                        var policyData = policyGrid.getData();

                        // begin the update
                        policyData.beginUpdate();

                        // add all users/groups
                        $.each(getTenantsToAdd($('#allowed-users')), function (_, user) {
                            // remove the user
                            policyData.addItem(user);
                        });
                        $.each(getTenantsToAdd($('#allowed-groups')), function (_, group) {
                            // remove the user
                            policyData.addItem(group);
                        });

                        // end the update
                        policyData.endUpdate();

                        // update the policy
                        updatePolicy();

                        // close the dialog
                        $('#search-users-dialog').modal('hide');
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
                            // close the dialog
                            $('#search-users-dialog').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // reset the search fields
                    $('#search-users-field').userSearchAutocomplete('reset').val('');

                    // clear the selected users/groups
                    $('#allowed-users, #allowed-groups').empty();
                }
            }
        });

        // listen for removal requests
        $(document).on('click', 'div.remove-allowed-entity', function () {
            $(this).closest('li').remove();
        });

        // configure the user auto complete
        $.widget('nf.userSearchAutocomplete', $.ui.autocomplete, {
            reset: function () {
                this.term = null;
            },
            _normalize: function (searchResults) {
                var items = [];
                items.push(searchResults);
                return items;
            },
            _renderMenu: function (ul, items) {
                // results are normalized into a single element array
                var searchResults = items[0];

                var allowedGroups = getAllAllowedGroups();
                var allowedUsers = getAllAllowedUsers();

                var self = this;
                $.each(searchResults.userGroups, function (_, tenant) {
                    // see if this match is not already selected
                    if ($.inArray(tenant.id, allowedGroups) === -1) {
                        self._renderGroup(ul, $.extend({
                            type: 'group'
                        }, tenant));
                    }
                });
                $.each(searchResults.users, function (_, tenant) {
                    // see if this match is not already selected
                    if ($.inArray(tenant.id, allowedUsers) === -1) {
                        self._renderUser(ul, $.extend({
                            type: 'user'
                        }, tenant));
                    }
                });

                // ensure there were some results
                if (ul.children().length === 0) {
                    ul.append('<li class="unset search-no-matches">No users matched the search terms</li>');
                }
            },
            _resizeMenu: function () {
                var ul = this.menu.element;
                ul.width($('#search-users-field').outerWidth() - 6);
            },
            _renderUser: function (ul, match) {
                var userContent = $('<a></a>').text(match.component.identity);
                return $('<li></li>').data('ui-autocomplete-item', match).append(userContent).appendTo(ul);
            },
            _renderGroup: function (ul, match) {
                var groupLabel = $('<span></span>').text(match.component.identity);
                var groupContent = $('<a></a>').append('<div class="fa fa-users" style="margin-right: 5px;"></div>').append(groupLabel);
                return $('<li></li>').data('ui-autocomplete-item', match).append(groupContent).appendTo(ul);
            }
        });

        // configure the autocomplete field
        $('#search-users-field').userSearchAutocomplete({
            minLength: 0,
            appendTo: '#search-users-results',
            position: {
                my: 'left top',
                at: 'left bottom',
                offset: '0 1'
            },
            source: function (request, response) {
                // create the search request
                $.ajax({
                    type: 'GET',
                    data: {
                        q: request.term
                    },
                    dataType: 'json',
                    url: config.urls.searchTenants
                }).done(function (searchResponse) {
                    response(searchResponse);
                });
            },
            select: function (event, ui) {
                addAllowedTenant(ui.item);

                // reset the search field
                $(this).val('');

                // stop event propagation
                return false;
            }
        });
    };

    /**
     * Gets all allowed groups including those already in the policy and those selected while searching (not yet saved).
     *
     * @returns {Array}
     */
    var getAllAllowedGroups = function () {
        var policyGrid = $('#policy-table').data('gridInstance');
        var policyData = policyGrid.getData();

        var userGroups = [];

        // consider existing groups in the policy table
        var items = policyData.getItems();
        $.each(items, function (_, item) {
            if (item.type === 'group') {
                userGroups.push(item.id);
            }
        });

        // also consider groups already selected in the search users dialog
        $.each(getTenantsToAdd($('#allowed-groups')), function (_, group) {
            userGroups.push(group.id);
        });

        return userGroups;
    };

    /**
     * Gets the user groups that will be added upon applying the changes.
     *
     * @param {jQuery} container
     * @returns {Array}
     */
    var getTenantsToAdd = function (container) {
        var tenants = [];

        // also consider groups already selected in the search users dialog
        container.children('li').each(function (_, allowedTenant) {
            var tenant = $(allowedTenant).data('tenant');
            if (nf.Common.isDefinedAndNotNull(tenant)) {
                tenants.push(tenant);
            }
        });

        return tenants;
    };

    /**
     * Gets all allowed users including those already in the policy and those selected while searching (not yet saved).
     *
     * @returns {Array}
     */
    var getAllAllowedUsers = function () {
        var policyGrid = $('#policy-table').data('gridInstance');
        var policyData = policyGrid.getData();

        var users = [];

        // consider existing users in the policy table
        var items = policyData.getItems();
        $.each(items, function (_, item) {
            if (item.type === 'user') {
                users.push(item.id);
            }
        });

        // also consider users already selected in the search users dialog
        $.each(getTenantsToAdd($('#allowed-users')), function (_, user) {
            users.push(user.id);
        });

        return users;
    };

    /**
     * Added the specified tenant to the listing of users/groups which will be added when applied.
     *
     * @param allowedTenant user/group to add
     */
    var addAllowedTenant = function (allowedTenant) {
        var allowedTenants = allowedTenant.type === 'user' ? $('#allowed-users') : $('#allowed-groups');

        // append the user
        var tenant = $('<span></span>').addClass('allowed-entity ellipsis').text(allowedTenant.component.identity).ellipsis();
        var tenantAction = $('<div></div>').addClass('remove-allowed-entity fa fa-trash');
        $('<li></li>').data('tenant', allowedTenant).append(tenant).append(tenantAction).appendTo(allowedTenants);
    };

    /**
     * Initializes the policy table.
     */
    var initPolicyTable = function () {
        $('#override-policy-dialog').modal({
            headerText: 'Override Policy',
            buttons: [{
                buttonText: 'Override',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // create the policy, copying if appropriate
                        createPolicy($('#copy-policy-radio-button').is(':checked'));

                        $(this).modal('hide');
                    }
                }
            }, {
                buttonText: 'Cancel',
                color: {
                    base: '#E3E8EB',
                    hover: '#C7D2D7',
                    text: '#004849'
                },
                handler: {
                    click: function () {
                        $(this).modal('hide');
                    }
                }
            }],
            handler: {
                close: function () {
                    // reset the radio button
                    $('#copy-policy-radio-button').prop('checked', true);
                }
            }
        });

        // create/add a policy
        $('#create-policy-link, #add-local-admin-link').on('click', function () {
            createPolicy(false);
        });

        // override a policy
        $('#override-policy-link').on('click', function () {
            $('#override-policy-dialog').modal('show');
        });

        // policy type listing
        $('#policy-type-list').combo({
            options: [
                nf.Common.getPolicyTypeListing('flow'),
                nf.Common.getPolicyTypeListing('controller'),
                nf.Common.getPolicyTypeListing('provenance'),
                nf.Common.getPolicyTypeListing('policies'),
                nf.Common.getPolicyTypeListing('tenants'),
                nf.Common.getPolicyTypeListing('site-to-site'),
                nf.Common.getPolicyTypeListing('system'),
                nf.Common.getPolicyTypeListing('proxy'),
                nf.Common.getPolicyTypeListing('counters')],
            select: function (option) {
                if (initialized) {
                    // record the policy type
                    $('#selected-policy-type').text(option.value);

                    // if the option is for a specific component
                    if (option.value === 'controller' || option.value === 'counters' || option.value === 'policies' || option.value === 'tenants') {
                        // update the policy target and let it relaod the policy
                        $('#controller-policy-target').combo('setSelectedOption', {
                            'value': 'read'
                        }).show();
                    } else {
                        $('#controller-policy-target').hide();

                        // record the action
                        if (option.value === 'proxy') {
                            $('#selected-policy-action').text('write');
                        } else {
                            $('#selected-policy-action').text('read');
                        }

                        // reload the policy
                        loadPolicy();
                    }
                }
            }
        });

        // controller policy target
        $('#controller-policy-target').combo({
            options: [{
                text: 'view',
                value: 'read'
            }, {
                text: 'modify',
                value: 'write'
            }],
            select: function (option) {
                if (initialized) {
                    // record the policy action
                    $('#selected-policy-action').text(option.value);

                    // reload the policy
                    loadPolicy();
                }
            }
        });
        
        // component policy target
        $('#component-policy-target').combo({
            options: [{
                text: 'view the component',
                value: 'read-component',
                description: 'Allows users to view component configuration details'
            }, {
                text: 'modify the component',
                value: 'write-component',
                description: 'Allows users to modify component configuration details'
            }, {
                text: 'view the data',
                value: 'read-data',
                description: 'Allows users to view metadata and content for this component through provenance data and flowfile queues in outbound connections'
            }, {
                text: 'modify the data',
                value: 'write-data',
                description: 'Allows users to empty flowfile queues in outbound connections and submit replays'
            }, {
                text: 'receive data via site-to-site',
                value: 'write-receive-data',
                description: 'Allows this port to receive data from these NiFi instances',
                disabled: true
            }, {
                text: 'send data via site-to-site',
                value: 'write-send-data',
                description: 'Allows this port to send data to these NiFi instances',
                disabled: true
            }, {
                text: 'view the policies',
                value: 'read-policies',
                description: 'Allows users to view the list of users who can view/modify this component'
            }, {
                text: 'modify the policies',
                value: 'write-policies',
                description: 'Allows users to modify the list of users who can view/modify this component'
            }],
            select: function (option) {
                if (initialized) {
                    var resource = $('#selected-policy-component-type').text();

                    if (option.value === 'read-component') {
                        $('#selected-policy-action').text('read');
                    } else if (option.value === 'write-component') {
                        $('#selected-policy-action').text('write');
                    } else if (option.value === 'read-data') {
                        $('#selected-policy-action').text('read');
                        resource = ('data/' + resource);
                    } else if (option.value === 'write-data') {
                        $('#selected-policy-action').text('write');
                        resource = ('data/' + resource);
                    } else if (option.value === 'read-policies') {
                        $('#selected-policy-action').text('read');
                        resource = ('policies/' + resource);
                    } else if (option.value === 'write-policies') {
                        $('#selected-policy-action').text('write');
                        resource = ('policies/' + resource);
                    } else if (option.value === 'write-receive-data') {
                        $('#selected-policy-action').text('write');
                        resource = 'data-transfer/input-ports';
                    } else if (option.value === 'write-send-data') {
                        $('#selected-policy-action').text('write');
                        resource = 'data-transfer/output-ports';
                    }

                    // set the resource
                    $('#selected-policy-type').text(resource);

                    // reload the policy
                    loadPolicy();
                }
            }
        });

        // function for formatting the user identity
        var identityFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';
            if (dataContext.type === 'group') {
                markup += '<div class="fa fa-users" style="margin-right: 5px;"></div>';
            }

            markup += dataContext.component.identity;

            return markup;
        };

        // function for formatting the actions column
        var actionFormatter = function (row, cell, value, columnDef, dataContext) {
            var markup = '';

            // see if the user has permissions for the current policy
            var currentEntity = $('#policy-table').data('policy');
            var isPolicyEditable = $('#delete-policy-button').is(':disabled') === false;
            if (currentEntity.permissions.canWrite === true && isPolicyEditable) {
                markup += '<div title="Remove" class="pointer delete-user fa fa-trash"></div>';
            }

            return markup;
        };

        // initialize the templates table
        var usersColumns = [
            {id: 'identity', name: 'User', sortable: true, resizable: true, formatter: identityFormatter},
            {id: 'actions', name: '&nbsp;', sortable: false, resizable: false, formatter: actionFormatter, width: 100, maxWidth: 100}
        ];
        var usersOptions = {
            forceFitColumns: true,
            enableTextSelectionOnCells: true,
            enableCellNavigation: true,
            enableColumnReorder: false,
            autoEdit: false
        };

        // initialize the dataview
        var policyData = new Slick.Data.DataView({
            inlineFilters: false
        });
        policyData.setItems([]);

        // initialize the sort
        sort({
            columnId: 'identity',
            sortAsc: true
        }, policyData);

        // initialize the grid
        var policyGrid = new Slick.Grid('#policy-table', policyData, usersColumns, usersOptions);
        policyGrid.setSelectionModel(new Slick.RowSelectionModel());
        policyGrid.registerPlugin(new Slick.AutoTooltips());
        policyGrid.setSortColumn('identity', true);
        policyGrid.onSort.subscribe(function (e, args) {
            sort({
                columnId: args.sortCol.id,
                sortAsc: args.sortAsc
            }, policyData);
        });

        // configure a click listener
        policyGrid.onClick.subscribe(function (e, args) {
            var target = $(e.target);

            // get the node at this row
            var item = policyData.getItem(args.row);

            // determine the desired action
            if (policyGrid.getColumns()[args.cell].id === 'actions') {
                if (target.hasClass('delete-user')) {
                    promptToRemoveUserFromPolicy(item);
                }
            }
        });

        // wire up the dataview to the grid
        policyData.onRowCountChanged.subscribe(function (e, args) {
            policyGrid.updateRowCount();
            policyGrid.render();

            // update the total number of displayed policy users
            $('#displayed-policy-users').text(args.current);
        });
        policyData.onRowsChanged.subscribe(function (e, args) {
            policyGrid.invalidateRows(args.rows);
            policyGrid.render();
        });

        // hold onto an instance of the grid
        $('#policy-table').data('gridInstance', policyGrid);

        // initialize the number of displayed items
        $('#displayed-policy-users').text('0');
    };

    /**
     * Sorts the specified data using the specified sort details.
     *
     * @param {object} sortDetails
     * @param {object} data
     */
    var sort = function (sortDetails, data) {
        // defines a function for sorting
        var comparer = function (a, b) {
            if(a.permissions.canRead && b.permissions.canRead) {
                var aString = nf.Common.isDefinedAndNotNull(a.component[sortDetails.columnId]) ? a.component[sortDetails.columnId] : '';
                var bString = nf.Common.isDefinedAndNotNull(b.component[sortDetails.columnId]) ? b.component[sortDetails.columnId] : '';
                return aString === bString ? 0 : aString > bString ? 1 : -1;
            } else {
                if (!a.permissions.canRead && !b.permissions.canRead){
                    return 0;
                }
                if(a.permissions.canRead){
                    return 1;
                } else {
                    return -1;
                }
            }
        };

        // perform the sort
        data.sort(comparer, sortDetails.sortAsc);
    };

    /**
     * Prompts for the removal of the specified user.
     *
     * @param item
     */
    var promptToRemoveUserFromPolicy = function (item) {
        nf.Dialog.showYesNoDialog({
            headerText: 'Update Policy',
            dialogContent: 'Remove \'' + nf.Common.escapeHtml(item.component.identity) + '\' from this policy?',
            yesHandler: function () {
                removeUserFromPolicy(item);
            }
        });
    };

    /**
     * Removes the specified item from the current policy.
     *
     * @param item
     */
    var removeUserFromPolicy = function (item) {
        var policyGrid = $('#policy-table').data('gridInstance');
        var policyData = policyGrid.getData();

        // begin the update
        policyData.beginUpdate();

        // remove the user
        policyData.deleteItem(item.id);

        // end the update
        policyData.endUpdate();

        // save the configuration
        updatePolicy();
    };

    /**
     * Prompts for the deletion of the selected policy.
     */
    var promptToDeletePolicy = function () {
        nf.Dialog.showYesNoDialog({
            headerText: 'Delete Policy',
            dialogContent: 'By deleting this policy, the permissions for this component will revert to the inherited policy if applicable.',
            yesText: 'Delete',
            noText: 'Cancel',
            yesHandler: function () {
                deletePolicy();
            }
        });
    };

    /**
     * Deletes the current policy.
     */
    var deletePolicy = function () {
        var currentEntity = $('#policy-table').data('policy');
        
        if (nf.Common.isDefinedAndNotNull(currentEntity)) {
            $.ajax({
                type: 'DELETE',
                url: currentEntity.uri + '?' + $.param(nf.Client.getRevision(currentEntity)),
                dataType: 'json'
            }).done(function () {
                loadPolicy();
            }).fail(function (xhr, status, error) {
                nf.Common.handleAjaxError(xhr, status, error);
                resetPolicy();
                loadPolicy();
            });
        } else {
            nf.Dialog.showOkDialog({
                headerText: 'Delete Policy',
                dialogContent: 'No policy selected'
            });
        }
    };

    /**
     * Gets the currently selected resource.
     */
    var getSelectedResourceAndAction = function () {
        var componentId = $('#selected-policy-component-id').text();
        var resource = $('#selected-policy-type').text();
        if (componentId !== '') {
            resource += ('/' + componentId);
        }

        return {
            'action': $('#selected-policy-action').text(),
            'resource': '/' + resource
        };
    };

    /**
     * Populates the table with the specified users and groups.
     *
     * @param users
     * @param userGroups
     */
    var populateTable = function (users, userGroups) {
        var policyGrid = $('#policy-table').data('gridInstance');
        var policyData = policyGrid.getData();

        // begin the update
        policyData.beginUpdate();

        var policyUsers = [];

        // add each user
        $.each(users, function (_, user) {
            policyUsers.push($.extend({
                type: 'user'
            }, user));
        });

        // add each group
        $.each(userGroups, function (_, group) {
            policyUsers.push($.extend({
                type: 'group'
            }, group));
        });

        // set the rows
        policyData.setItems(policyUsers);

        // end the update
        policyData.endUpdate();

        // re-sort and clear selection after updating
        policyData.reSort();
        policyGrid.invalidate();
        policyGrid.getSelectionModel().setSelectedRows([]);
    };

    /**
     * Converts the specified resource into human readable form.
     *
     * @param resource
     */
    var getResourceMessage = function (resource) {
        if (resource === '/policies') {
            return $('<span>Showing effective policy inherited from all policies.</span>');
        } else if (resource === '/controller') {
            return $('<span>Showing effective policy inherited from the controller.</span>');
        } else {
            // extract the group id
            var processGroupId = nf.Common.substringAfterLast(resource, '/');
            var processGroupName = processGroupId;

            // attempt to resolve the group name
            var breadcrumbs = nf.ng.Bridge.injector.get('breadcrumbsCtrl').getBreadcrumbs();
            $.each(breadcrumbs, function (_, breadcrumbEntity) {
                if (breadcrumbEntity.id === processGroupId) {
                    processGroupName = breadcrumbEntity.label;
                    return false;
                }
            });

            // build the mark up
            return $('<span>Showing effective policy inherited from Process Group </span>').append($('<span class="link"></span>').text(processGroupName).on('click', function () {
                // close the shell
                $('#shell-close-button').click();

                // load the correct group and unselect everything if necessary
                nf.CanvasUtils.enterGroup(processGroupId).done(function () {
                    nf.CanvasUtils.getSelection().classed('selected', false);

                    // inform Angular app that values have changed
                    nf.ng.Bridge.digest();
                });
            })).append('<span>.</span>');
        }
    };

    /**
     * Populates the specified policy.
     *
     * @param policyEntity
     */
    var populatePolicy = function (policyEntity) {
        var policy = policyEntity.component;

        // get the currently selected policy
        var resourceAndAction = getSelectedResourceAndAction();

        // reset of the policy message
        resetPolicyMessage();

        // store the current policy version
        $('#policy-table').data('policy', policyEntity);

        // see if the policy is for this resource
        if (resourceAndAction.resource === policy.resource) {
            // allow remove when policy is not inherited
            $('#delete-policy-button').prop('disabled', policyEntity.permissions.canWrite === false);

            // allow modification if allowed
            $('#new-policy-user-button').prop('disabled', policyEntity.permissions.canWrite === false);
        } else {
            $('#policy-message').append(getResourceMessage(policy.resource));

            // policy is inherited, we do not know if the user has permissions to modify the desired policy... show button and let server decide
            $('#override-policy-message').show();

            // do not support policy deletion/modification
            $('#delete-policy-button').prop('disabled', true);
            $('#new-policy-user-button').prop('disabled', true);
        }

        // populate the table
        populateTable(policy.users, policy.userGroups);
    };

    /**
     * Loads the configuration for the specified process group.
     */
    var loadPolicy = function () {
        var resourceAndAction = getSelectedResourceAndAction();

        var policyDeferred;
        if (resourceAndAction.resource.startsWith('/policies')) {
            $('#admin-policy-message').show();

            policyDeferred = $.Deferred(function (deferred) {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/policies/' + resourceAndAction.action + resourceAndAction.resource,
                    dataType: 'json'
                }).done(function (policyEntity) {
                    // update the refresh timestamp
                    $('#policy-last-refreshed').text(policyEntity.generated);

                    // ensure appropriate actions for the loaded policy
                    if (policyEntity.permissions.canRead === true) {
                        var policy = policyEntity.component;

                        // if the return policy is for the desired policy (not inherited, show it)
                        if (resourceAndAction.resource === policy.resource) {
                            // populate the policy details
                            populatePolicy(policyEntity);
                        } else {
                            // reset the policy
                            resetPolicy();

                            // show an appropriate message
                            $('#policy-message').text('No component specific administrators.');

                            // we don't know if the user has permissions to the desired policy... show create button and allow the server to decide
                            $('#add-local-admin-message').show();
                        }
                    } else {
                        // reset the policy
                        resetPolicy();

                        // show an appropriate message
                        $('#policy-message').text('No component specific administrators.');

                        // we don't know if the user has permissions to the desired policy... show create button and allow the server to decide
                        $('#add-local-admin-message').show();
                    }

                    deferred.resolve();
                }).fail(function (xhr, status, error) {
                    if (xhr.status === 404) {
                        // reset the policy
                        resetPolicy();

                        // show an appropriate message
                        $('#policy-message').text('No component specific administrators.');

                        // we don't know if the user has permissions to the desired policy... show create button and allow the server to decide
                        $('#add-local-admin-message').show();

                        deferred.resolve();
                    } else if (xhr.status === 403) {
                        // reset the policy
                        resetPolicy();

                        // show an appropriate message
                        $('#policy-message').text('Not authorized to access the policy for the specified resource.');

                        deferred.resolve();
                    } else {
                        // reset the policy
                        resetPolicy();

                        deferred.reject();
                        nf.Common.handleAjaxError(xhr, status, error);
                    }
                });
            }).promise();
        } else {
            $('#admin-policy-message').hide();

            policyDeferred = $.Deferred(function (deferred) {
                $.ajax({
                    type: 'GET',
                    url: '../nifi-api/policies/' + resourceAndAction.action + resourceAndAction.resource,
                    dataType: 'json'
                }).done(function (policyEntity) {
                    // return OK so we either have access to the policy or we don't have access to an inherited policy

                    // update the refresh timestamp
                    $('#policy-last-refreshed').text(policyEntity.generated);

                    // ensure appropriate actions for the loaded policy
                    if (policyEntity.permissions.canRead === true) {
                        // populate the policy details
                        populatePolicy(policyEntity);
                    } else {
                        // reset the policy
                        resetPolicy();

                        // since we cannot read, the policy may be inherited or not... we cannot tell
                        $('#policy-message').text('Not authorized to view the policy.');

                        // allow option to override because we don't know if it's supported or not
                        $('#override-policy-message').show();
                    }

                    deferred.resolve();
                }).fail(function (xhr, status, error) {
                    if (xhr.status === 404) {
                        // reset the policy
                        resetPolicy();

                        // show an appropriate message
                        $('#policy-message').text('No policy for the specified resource.');

                        // we don't know if the user has permissions to the desired policy... show create button and allow the server to decide
                        $('#new-policy-message').show();

                        deferred.resolve();
                    } else if (xhr.status === 403) {
                        // reset the policy
                        resetPolicy();

                        // show an appropriate message
                        $('#policy-message').text('Not authorized to access the policy for the specified resource.');

                        deferred.resolve();
                    } else {
                        resetPolicy();

                        deferred.reject();
                        nf.Common.handleAjaxError(xhr, status, error);
                    }
                });
            }).promise();
        }

        return policyDeferred;
    };

    /**
     * Creates a new policy for the current selection.
     *
     * @param copyInheritedPolicy   Whether or not to copy the inherited policy
     */
    var createPolicy = function (copyInheritedPolicy) {
        var resourceAndAction = getSelectedResourceAndAction();

        var users = [];
        var userGroups = [];
        if (copyInheritedPolicy === true) {
            var policyGrid = $('#policy-table').data('gridInstance');
            var policyData = policyGrid.getData();

            var items = policyData.getItems();
            $.each(items, function (_, item) {
                var itemCopy = $.extend({}, item);

                if (itemCopy.type === 'user') {
                    users.push(itemCopy);
                } else {
                    userGroups.push(itemCopy);
                }

                // remove the type as it was added client side to render differently and is not part of the actual schema
                delete itemCopy.type;
            });
        }

        var entity = {
            'revision': nf.Client.getRevision({
                'revision': {
                    'version': 0
                }
            }),
            'component': {
                'action': resourceAndAction.action,
                'resource': resourceAndAction.resource,
                'users': users,
                'userGroups': userGroups
            }
        };

        $.ajax({
            type: 'POST',
            url: '../nifi-api/policies',
            data: JSON.stringify(entity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (policyEntity) {
            // ensure appropriate actions for the loaded policy
            if (policyEntity.permissions.canRead === true) {
                // populate the policy details
                populatePolicy(policyEntity);
            } else {
                // the request succeeded but we don't have access to the policy... reset/reload the policy
                resetPolicy();
                loadPolicy();
            }
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Updates the policy for the current selection.
     */
    var updatePolicy = function () {
        var policyGrid = $('#policy-table').data('gridInstance');
        var policyData = policyGrid.getData();

        var users = [];
        var userGroups = [];

        var items = policyData.getItems();
        $.each(items, function (_, item) {
            var itemCopy = $.extend({}, item);

            if (itemCopy.type === 'user') {
                users.push(itemCopy);
            } else {
                userGroups.push(itemCopy);
            }

            // remove the type as it was added client side to render differently and is not part of the actual schema
            delete itemCopy.type;
        });

        var currentEntity = $('#policy-table').data('policy');
        if (nf.Common.isDefinedAndNotNull(currentEntity)) {
            var entity = {
                'revision': nf.Client.getRevision(currentEntity),
                'component': {
                    'id': currentEntity.id,
                    'users': users,
                    'userGroups': userGroups
                }
            };
    
            $.ajax({
                type: 'PUT',
                url: currentEntity.uri,
                data: JSON.stringify(entity),
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (policyEntity) {
                // ensure appropriate actions for the loaded policy
                if (policyEntity.permissions.canRead === true) {
                    // populate the policy details
                    populatePolicy(policyEntity);
                } else {
                    // the request succeeded but we don't have access to the policy... reset/reload the policy
                    resetPolicy();
                    loadPolicy();
                }
            }).fail(function (xhr, status, error) {
                nf.Common.handleAjaxError(xhr, status, error);
                resetPolicy();
                loadPolicy();
            }).always(function () {
                nf.Canvas.reload({
                    'transition': true
                });
            });
        } else {
            nf.Dialog.showOkDialog({
                headerText: 'Update Policy',
                dialogContent: 'No policy selected'
            });
        }
    };

    /**
     * Shows the process group configuration.
     */
    var showPolicy = function () {
        // show the configuration dialog
        nf.Shell.showContent('#policy-management').always(function () {
            reset();
        });

        // adjust the table size
        nf.PolicyManagement.resetTableSize();
    };

    /**
     * Reset the policy message.
     */
    var resetPolicyMessage = function () {
        $('#policy-message').text('').empty();
        $('#new-policy-message').hide();
        $('#override-policy-message').hide();
        $('#add-local-admin-message').hide();
    };

    /**
     * Reset the policy.
     */
    var resetPolicy = function () {
        resetPolicyMessage();

        // reset button state
        $('#delete-policy-button').prop('disabled', true);
        $('#new-policy-user-button').prop('disabled', true);

        // reset the current policy
        $('#policy-table').removeData('policy');

        // populate the table with no users
        populateTable([], []);
    }

    /**
     * Resets the policy management dialog.
     */
    var reset = function () {
        resetPolicy();

        // clear the selected policy details
        $('#selected-policy-type').text('');
        $('#selected-policy-action').text('');
        $('#selected-policy-component-id').text('');
        $('#selected-policy-component-type').text('');
        
        // clear the selected component details
        $('div.policy-selected-component-container').hide();
    };

    return {
        /**
         * Initializes the settings page.
         */
        init: function () {
            initAddTenantToPolicyDialog();
            initPolicyTable();

            $('#policy-refresh-button').on('click', function () {
                loadPolicy();
            });

            // reset the policy to initialize
            resetPolicy();

            // mark as initialized
            initialized = true;
        },

        /**
         * Update the size of the grid based on its container's current size.
         */
        resetTableSize: function () {
            var policyTable = $('#policy-table');
            if (policyTable.is(':visible')) {
                var policyGrid = policyTable.data('gridInstance');
                if (nf.Common.isDefinedAndNotNull(policyGrid)) {
                    policyGrid.resizeCanvas();
                }
            }
        },

        /**
         * Shows the controller service policy.
         *
         * @param d
         */
        showControllerServicePolicy: function (d) {
            // reset the policy message
            resetPolicyMessage();

            // update the policy controls visibility
            $('#component-policy-controls').show();
            $('#global-policy-controls').hide();

            // update the visibility
            if (d.permissions.canRead === true) {
                $('#policy-selected-controller-service-container div.policy-selected-component-name').text(d.component.name);
            } else {
                $('#policy-selected-controller-service-container div.policy-selected-component-name').text(d.id);
            }
            $('#policy-selected-controller-service-container').show();

            // populate the initial resource
            $('#selected-policy-component-id').text(d.id);
            $('#selected-policy-component-type').text('controller-services');
            $('#component-policy-target')
                .combo('setOptionEnabled', {
                    value: 'write-receive-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'write-send-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'read-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'write-data'
                }, false)
                .combo('setSelectedOption', {
                    value: 'read-component'
                });

            return loadPolicy().always(showPolicy);
        },

        /**
         * Shows the reporting task policy.
         *
         * @param d
         */
        showReportingTaskPolicy: function (d) {
            // reset the policy message
            resetPolicyMessage();

            // update the policy controls visibility
            $('#component-policy-controls').show();
            $('#global-policy-controls').hide();

            // update the visibility
            if (d.permissions.canRead === true) {
                $('#policy-selected-reporting-task-container div.policy-selected-component-name').text(d.component.name);
            } else {
                $('#policy-selected-reporting-task-container div.policy-selected-component-name').text(d.id);
            }
            $('#policy-selected-reporting-task-container').show();
            
            // populate the initial resource
            $('#selected-policy-component-id').text(d.id);
            $('#selected-policy-component-type').text('reporting-tasks');
            $('#component-policy-target')
                .combo('setOptionEnabled', {
                    value: 'write-receive-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'write-send-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'read-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'write-data'
                }, false)
                .combo('setSelectedOption', {
                    value: 'read-component'
                });

            return loadPolicy().always(showPolicy);
        },

        /**
         * Shows the template policy.
         *
         * @param d
         */
        showTemplatePolicy: function (d) {
            // reset the policy message
            resetPolicyMessage();

            // update the policy controls visibility
            $('#component-policy-controls').show();
            $('#global-policy-controls').hide();

            // update the visibility
            if (d.permissions.canRead === true) {
                $('#policy-selected-template-container div.policy-selected-component-name').text(d.template.name);
            } else {
                $('#policy-selected-template-container div.policy-selected-component-name').text(d.id);
            }
            $('#policy-selected-template-container').show();

            // populate the initial resource
            $('#selected-policy-component-id').text(d.id);
            $('#selected-policy-component-type').text('templates');
            $('#component-policy-target')
                .combo('setOptionEnabled', {
                    value: 'write-receive-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'write-send-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'read-data'
                }, false)
                .combo('setOptionEnabled', {
                    value: 'write-data'
                }, false)
                .combo('setSelectedOption', {
                    value: 'read-component'
                });

            return loadPolicy().always(showPolicy);
        },

        /**
         * Shows the component policy dialog.
         */
        showComponentPolicy: function (selection) {
            // reset the policy message
            resetPolicyMessage();

            // update the policy controls visibility
            $('#component-policy-controls').show();
            $('#global-policy-controls').hide();

            // update the visibility
            $('#policy-selected-component-container').show();
            
            var resource;
            if (selection.empty()) {
                $('#selected-policy-component-id').text(nf.Canvas.getGroupId());
                resource = 'process-groups';

                // disable site to site option
                $('#component-policy-target')
                    .combo('setOptionEnabled', {
                        value: 'write-receive-data'
                    }, false)
                    .combo('setOptionEnabled', {
                        value: 'write-send-data'
                    }, false)
                    .combo('setOptionEnabled', {
                        value: 'read-data'
                    }, true)
                    .combo('setOptionEnabled', {
                        value: 'write-data'
                    }, true);
            } else {
                var d = selection.datum();
                $('#selected-policy-component-id').text(d.id);

                if (nf.CanvasUtils.isProcessor(selection)) {
                    resource = 'processors';
                } else if (nf.CanvasUtils.isProcessGroup(selection)) {
                    resource = 'process-groups';
                } else if (nf.CanvasUtils.isInputPort(selection)) {
                    resource = 'input-ports';
                } else if (nf.CanvasUtils.isOutputPort(selection)) {
                    resource = 'output-ports';
                } else if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                    resource = 'remote-process-groups';
                } else if (nf.CanvasUtils.isLabel(selection)) {
                    resource = 'labels';
                } else if (nf.CanvasUtils.isFunnel(selection)) {
                    resource = 'funnels';
                }

                // enable site to site option
                $('#component-policy-target')
                    .combo('setOptionEnabled', {
                        value: 'write-receive-data'
                    }, nf.CanvasUtils.isInputPort(selection) && nf.Canvas.getParentGroupId() === null)
                    .combo('setOptionEnabled', {
                        value: 'write-send-data'
                    }, nf.CanvasUtils.isOutputPort(selection) && nf.Canvas.getParentGroupId() === null)
                    .combo('setOptionEnabled', {
                        value: 'read-data'
                    }, !nf.CanvasUtils.isLabel(selection))
                    .combo('setOptionEnabled', {
                        value: 'write-data'
                    }, !nf.CanvasUtils.isLabel(selection));
            }

            // populate the initial resource
            $('#selected-policy-component-type').text(resource);
            $('#component-policy-target').combo('setSelectedOption', {
                value: 'read-component'
            });

            return loadPolicy().always(showPolicy);
        },

        /**
         * Shows the global policies dialog.
         */
        showGlobalPolicies: function () {
            // reset the policy message
            resetPolicyMessage();

            // update the policy controls visibility
            $('#component-policy-controls').hide();
            $('#global-policy-controls').show();

            // reload the current policies
            var policyType = $('#policy-type-list').combo('getSelectedOption').value;
            $('#selected-policy-type').text(policyType);

            if (policyType === 'controller') {
                $('#selected-policy-action').text($('#controller-policy-target').combo('getSelectedOption').value);
            } else if (policyType === 'proxy') {
                $('#selected-policy-action').text('write');
            } else {
                $('#selected-policy-action').text('read');
            }

            return loadPolicy().always(showPolicy);
        }
    };
}());