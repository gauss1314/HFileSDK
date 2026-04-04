package com.hfile;

public class HFileSDK {

    static {
        if (!Boolean.getBoolean("hfilesdk.native.loaded")) {
            System.loadLibrary("hfilesdk");
        }
    }

    public static final int OK = 0;

    public native int convert(String arrowPath,
                              String hfilePath,
                              String tableName,
                              String rowKeyRule);

    public native String getLastResult();

    public native int configure(String configJson);
}
