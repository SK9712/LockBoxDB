package com.lockboxdb.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBUtils {

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

    public static byte[] serialize(Object data) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(data);
            return byteArrayOutputStream.toByteArray();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static Object deserialize(byte[] data) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            return objectInputStream.readObject();
        } catch (Exception ex) {

            ex.printStackTrace();
        }
        return null;
    }
}
