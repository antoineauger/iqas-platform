package fr.isae.iqas.model.request;

import java.util.Date;

public class State {
    private Status status;
    private Date start_date;
    private Date end_date;

    public State(Status status, Date start_date) {
        this.status = status;
        this.start_date = start_date;
        this.end_date = start_date;
    }

    public State(Status status, Date start_date, Date end_date) {
        this.status = status;
        this.start_date = start_date;
        this.end_date = end_date;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Date getStart_date() {
        return start_date;
    }

    public void setStart_date(Date start_date) {
        this.start_date = start_date;
    }

    public Date getEnd_date() {
        return end_date;
    }

    public void setEnd_date(Date end_date) {
        this.end_date = end_date;
    }
}
