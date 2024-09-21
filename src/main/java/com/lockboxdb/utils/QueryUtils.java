package com.lockboxdb.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryUtils {

    public static String parseSingleQuotedString(String input) {
        // Regex pattern to match a single-quoted string
        Pattern pattern = Pattern.compile("'([^']*)'");
        Matcher matcher = pattern.matcher(input);

        // Find and return the first match
        if (matcher.find()) {
            return matcher.group(1); // The content between the single quotes
        }
        return null; // No match found
    }

}
