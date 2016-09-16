package fr.isae.iqas.server;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

/**
 * Created by an.auger on 16/09/2016.
 */
public class RESTServer extends AllDirectives {

    public Route createRoute() {
        // This handler generates responses to `/hello?name=XXX` requests
        Route helloRoute =
                parameterOptional("name", optName -> {
                    String name = optName.orElse("Mister X");
                    return complete("Hello " + name + "!");
                });

        return
                // here the complete behavior for this server is defined
                route(
                        path("ping", () ->
                                route(
                                        get(() -> route(
                                                complete("PONG GET!")
                                        )),
                                        post(() -> route(
                                                complete("PONG POST!")
                                        ))
                                )
                        ),
                        get(() -> route(
                                // matches the empty path
                                pathSingleSlash(() ->
                                        // return a constant string with a certain content type
                                        complete(HttpEntities.create(ContentTypes.TEXT_HTML_UTF8, "<html><body>Hello world!</body></html>"))
                                ),
                                path("ping", () ->
                                        // return a simple `text/plain` response
                                        complete("PONG!")
                                ),
                                path("hello", () ->
                                        // uses the route defined above
                                        helloRoute
                                )
                        )),
                        get(() -> complete("Unknown endpoint.")),
                        post(() -> complete("Unknown endpoint."))
                );
    }

}
