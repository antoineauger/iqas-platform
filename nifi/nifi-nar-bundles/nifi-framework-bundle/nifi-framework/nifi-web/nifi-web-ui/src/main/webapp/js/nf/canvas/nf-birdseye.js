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

nf.Birdseye = (function () {

    var birdseyeGroup;
    var componentGroup;

    // refreshes the birdseye
    var refresh = function (components) {
        var translate = nf.Canvas.View.translate();
        var scale = nf.Canvas.View.scale();

        // scale the translation
        translate = [translate[0] / scale, translate[1] / scale];

        // get the bounding box for the graph and convert into canvas space
        var graphBox = d3.select('#canvas').node().getBoundingClientRect();
        var graphLeft = (graphBox.left / scale) - translate[0];
        var graphTop = ((graphBox.top - nf.Canvas.CANVAS_OFFSET) / scale) - translate[1];
        var graphRight = (graphBox.right / scale) - translate[0];
        var graphBottom = (graphBox.bottom / scale) - translate[1];

        // get the screen bounding box and convert into canvas space
        var canvasContainer = $('#canvas-container');
        var screenWidth = canvasContainer.width() / scale;
        var screenHeight = canvasContainer.height() / scale;
        var screenLeft = -translate[0];
        var screenTop = -translate[1];
        var screenRight = screenLeft + screenWidth;
        var screenBottom = screenTop + screenHeight;

        // determine the maximum size of the canvas given the graph and the visible portion on the screen
        var canvasLeft = Math.min(graphLeft, screenLeft);
        var canvasTop = Math.min(graphTop, screenTop);
        var canvasRight = Math.max(graphRight, screenRight);
        var canvasBottom = Math.max(graphBottom, screenBottom);
        var canvasWidth = canvasRight - canvasLeft;
        var canvasHeight = canvasBottom - canvasTop;

        // get the width/height we have to work with
        var birdseye = $('#birdseye');
        var birdseyeWidth = birdseye.width();
        var birdseyeHeight = birdseye.height();

        // determine the appropriate scale for the birdseye (min scale to accomodate total width and height)
        var birdseyeWidthScale = birdseyeWidth / canvasWidth;
        var birdseyeHeightScale = birdseyeHeight / canvasHeight;
        var birdseyeScale = Math.min(birdseyeWidthScale, birdseyeHeightScale);

        // calculate the translation for the birdseye and the brush
        var birdseyeTranslate = [0, 0];
        var brushTranslate = [0, 0];
        if (translate[0] < 0 && translate[1] < 0) {
            birdseyeTranslate = [0, 0];
            brushTranslate = [-translate[0], -translate[1]];
        } else if (translate[0] >= 0 && translate[1] < 0) {
            birdseyeTranslate = [translate[0], 0];
            brushTranslate = [0, -translate[1]];
        } else if (translate[0] < 0 && translate[1] >= 0) {
            birdseyeTranslate = [0, translate[1]];
            brushTranslate = [-translate[0], 0];
        } else {
            birdseyeTranslate = [translate[0], translate[1]];
            brushTranslate = [0, 0];
        }

        // offset in case the graph has positive/negative coordinates and panning appropriately
        var offsetX = 0;
        var left = -graphLeft;
        if (translate[0] < 0) {
            if (translate[0] < left) {
                offsetX = left;
            } else {
                offsetX = left - (left - translate[0]);
            }
        } else {
            if (translate[0] < left) {
                offsetX = left - translate[0];
            }
        }
        var offsetY = 0;
        var top = -graphTop;
        if (translate[1] < 0) {
            if (translate[1] < top) {
                offsetY = top;
            } else {
                offsetY = top - (top - translate[1]);
            }
        } else {
            if (translate[1] < top) {
                offsetY = top - translate[1];
            }
        }

        // adjust the translations of the birdseye and brush to account for the offset
        birdseyeTranslate = [birdseyeTranslate[0] + offsetX, birdseyeTranslate[1] + offsetY];
        brushTranslate = [brushTranslate[0] + offsetX, brushTranslate[1] + offsetY];

        // update the birdseye
        birdseyeGroup.attr('transform', 'scale(' + birdseyeScale + ')');
        componentGroup.attr('transform', 'translate(' + birdseyeTranslate + ')');

        // update the brush
        d3.select('rect.birdseye-brush')
            .attr({
                'width': screenWidth,
                'height': screenHeight,
                'stroke-width': (2 / birdseyeScale),
                'transform': function (d) {
                    d.x = brushTranslate[0];
                    d.y = brushTranslate[1];

                    return 'translate(' + brushTranslate + ')';
                }
            });

        // redraw the canvas
        var canvasElement = d3.select('#birdseye-canvas').node();
        var context = canvasElement.getContext('2d');
        context.save();

        // clear the current canvas
        context.setTransform(1, 0, 0, 1, 0, 0);
        context.clearRect(0, 0, canvasElement.width, canvasElement.height);

        context.restore();
        context.save();

        // apply the current transformation
        context.translate(birdseyeTranslate[0] * birdseyeScale, birdseyeTranslate[1] * birdseyeScale);
        context.scale(birdseyeScale, birdseyeScale);

        // labels
        $.each(components.labels, function (_, d) {
            var color = nf.Label.defaultColor();

            if (d.accessPolicy.canRead) {
                // use the specified color if appropriate
                if (nf.Common.isDefinedAndNotNull(d.component.style['background-color'])) {
                    color = d.component.style['background-color'];
                }
            }

            context.fillStyle = color;
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // funnels
        context.fillStyle = '#ad9897';
        $.each(components.funnels, function (_, d) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // ports
        context.fillStyle = '#bbdcde';
        $.each(components.ports, function (_, d) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // remote process groups
        context.fillStyle = '#728e9b';
        $.each(components.remoteProcessGroups, function (_, d) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // process groups
        $.each(components.processGroups, function (_, d) {
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        // processors
        $.each(components.processors, function (_, d) {
            var color = '#dde4eb';

            if (d.accessPolicy.canRead) {
                // use the specified color if appropriate
                if (nf.Common.isDefinedAndNotNull(d.component.style['background-color'])) {
                    color = d.component.style['background-color'];
                }
            }

            context.fillStyle = color;
            context.fillRect(d.position.x, d.position.y, d.dimensions.width, d.dimensions.height);
        });

        context.restore();
    };

    // whether or not the birdseye is open, don't adjust unless necessary
    var visible = true;

    return {
        init: function () {
            var birdseye = $('#birdseye');

            d3.select('#birdseye').append('canvas')
                .attr('id', 'birdseye-canvas')
                .attr('width', birdseye.width())
                .attr('height', birdseye.height());

            // build the birdseye svg
            var svg = d3.select('#birdseye').append('svg')
                .attr('width', birdseye.width())
                .attr('height', birdseye.height());

            // group birdseye components together
            birdseyeGroup = svg.append('g')
                .attr('class', 'birdseye');

            // processor in the birdseye
            componentGroup = birdseyeGroup.append('g')
                .attr('pointer-events', 'none');

            // define the brush drag behavior
            var brush = d3.behavior.drag()
                .origin(function (d) {
                    return {
                        x: d.x,
                        y: d.y
                    };
                })
                .on('dragstart', function () {
                    // hide the context menu
                    nf.ContextMenu.hide();
                })
                .on('drag', function (d) {
                    d.x += d3.event.dx;
                    d.y += d3.event.dy;

                    // update the location of the brush
                    d3.select(this).attr('transform', function () {
                        return 'translate(' + d.x + ', ' + d.y + ')';
                    });
                    // get the current transformation
                    var scale = nf.Canvas.View.scale();
                    var translate = nf.Canvas.View.translate();

                    // update the translation according to the delta
                    translate = [(-d3.event.dx * scale) + translate[0], (-d3.event.dy * scale) + translate[1]];

                    // record the current transforms
                    nf.Canvas.View.translate(translate);

                    // refresh the canvas
                    nf.Canvas.View.refresh({
                        persist: false,
                        transition: false,
                        refreshComponents: false,
                        refreshBirdseye: false
                    });
                })
                .on('dragend', function () {
                    // update component visibility
                    nf.Canvas.View.updateVisibility();

                    // persist the users view
                    nf.CanvasUtils.persistUserView();

                    // refresh the birdseye
                    nf.Birdseye.refresh();
                });

            // context area
            birdseyeGroup.append('g')
                .attr({
                    'pointer-events': 'all',
                    'class': 'birdseye-brush-container'
                })
                .append('rect')
                .attr('class', 'birdseye-brush moveable')
                .datum({
                    x: 0,
                    y: 0
                })
                .call(brush);
        },

        /**
         * Handles rendering of the birdseye tool.
         */
        refresh: function () {
            if (visible) {
                refresh(nf.Graph.get());
            }
        },

        /**
         * Function that needs to be call when the birdseye visibility changes.
         *
         * @param {boolean} isVisible
         */
        updateBirdseyeVisibility: function (isVisible) {
            var birdseye = $('#birdseye');

            // update the outline collapse icon
            if (isVisible) {
                // show the outline
                birdseye.show();
                visible = true;

                // refresh the birdseye as it may have changed
                refresh(nf.Graph.get());
            } else {
                // hide the outline
                birdseye.hide();
                visible = false;
            }
        }
    };
}());