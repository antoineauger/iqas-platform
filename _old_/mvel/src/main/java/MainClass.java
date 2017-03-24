import org.mvel2.MVEL;

import java.io.File;
import java.io.IOException;

/**
 * Created by an.auger on 09/03/2017.
 */
public class MainClass {

    public static void main(String[] args) throws IOException {
        System.out.println(MainClass.class.getResource("input.mvel"));

        ObjectTest t = (ObjectTest) MVEL.evalFile(new File("/Users/an.auger/Desktop/mvel/src/main/resources/input.mvel"));
        System.out.println(t.getName());
    }

}
