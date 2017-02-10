package fr.isae.iqas.model.message;

import fr.isae.iqas.model.request.Request;

/**
 * Created by an.auger on 10/02/2017.
 */
public class RESTRequestMsg {
    public enum RequestSubject {
        GET,
        DELETE,
        POST,
        PUT
    }

    private RequestSubject requestSubject;
    private Request request;

    public RESTRequestMsg(RequestSubject requestSubject, Request request) {
        this.requestSubject = requestSubject;
        this.request = request;
    }

    public RequestSubject getRequestSubject() {
        return requestSubject;
    }

    public Request getRequest() {
        return request;
    }

}
