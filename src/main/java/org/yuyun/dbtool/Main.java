package org.yuyun.dbtool;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] _args) {
        Map<String, String> args = new HashMap<>();

        for(int i=1; i<_args.length; i++) {
            if(_args[i].startsWith("--")) {
                String arg = _args[i].substring(2);
                i += 1;
                if(i >= _args.length)
                    throw new RuntimeException(String.format("argument %s has no value specified", arg));
                String value = _args[i];
                if(value.startsWith("base64:"))
                    value = new String(Base64.getDecoder().decode(value.substring(7)), StandardCharsets.UTF_8);
                args.put(arg, value);
            }
            else if(_args[i].startsWith("-")) {
                String arg = _args[i].substring(1);
                args.put(arg, "yes");
            }
            else
                throw new RuntimeException(String.format("Unrecognized parameter %s", _args[i]));
        }

        if(!args.containsKey("action"))
            args.put("action", _args[0]);

        try {
            Processor.process(args);
        } catch (Throwable e) {
            Processor.printMsg(e);
            System.exit(1);
        }
    }
}

