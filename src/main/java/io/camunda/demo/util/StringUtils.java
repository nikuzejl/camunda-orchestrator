package io.camunda.demo.util;

public class StringUtils {
    public static String stripQuotes(String v) {
        return v == null ? null : v.replace("\"", "");
    }
}
