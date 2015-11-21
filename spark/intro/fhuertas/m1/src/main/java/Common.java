/**
 * Created by Francisco Huertas on 10/11/15.
 */

import com.google.common.collect.Maps;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class Common {
    private static final Logger LOGGER = LogManager.getLogger(Common.class.getName());

    public static final String FILE_PATH_ATTR = "file";
    public static final String MASTER = "master";


    public static Map<String,String> readProperties (String []args) {
        Map<String,String> properties = Maps.newHashMap();
        for (int i = 0; i < args.length;i++) {

            if (args[i].indexOf("--") == 0){
                String arg = args[i].substring(2);
                try {
                    if (args[i+1].indexOf("--") != 0) {
                        i++;
                        properties.put(arg,args[i]);
                    } else {
                        properties.put(arg,"");
                    }
                } catch (ArrayIndexOutOfBoundsException ex) {
                    // If out of array this attribute not has value
                    properties.put(arg,"");
                }
            }
        }
        return properties;

    }
}
