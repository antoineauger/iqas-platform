package fr.isae.iqas.model.observation;

import java.sql.Timestamp;

/**
 * Created by an.auger on 08/02/2017.
 */
public class Knowledge extends Information {
    // TODO refactor

    public Knowledge(Information information) {
        super(information);
    }

    public Knowledge(RawData rawData) {
        super(rawData);
    }

    public Knowledge(String date, String value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
    }

    public Knowledge(Timestamp date, Double value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
    }

    public void test() {




        /*for (Iterator<Resource> i = m.l.listRDFTypes(false); i.hasNext(); ) {
            System.out.println( iqas.getURI() + " is asserted in class " + i.next() );
        }

        // list the inferred types
        for (Iterator<Resource> i = sensor01.listRDFTypes(false); i.hasNext(); ) {
            System.out.println( iqas.getURI() + " is inferred to be in class " + i.next() );
        }

        // list the properties
        for (StmtIterator i = sensor01.listProperties(); i.hasNext(); ) {
            System.out.println( i.next() );
        }*/
    }


}
