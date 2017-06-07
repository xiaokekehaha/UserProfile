/**
 * Created by sean on 5/26/17.
 */
import org.apache.hadoop.hive.ql.exec.UDF;


import java.security.MessageDigest;




public  class udf_lps_did extends UDF {
    public static String evaluate( String s) {
        String strResult = null;
        if (s == null || s.length() == 0 || "".equals(s)) {
            return null;
        } else {
            try {
                MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                messageDigest.update("f977c2863591a9691a18000af370c519".getBytes());
                messageDigest.update(s.getBytes());
                byte byteBuffer[] = messageDigest.digest();

                StringBuffer strHexString = new StringBuffer();
                for (int i = 0; i < byteBuffer.length; i++) {
                    String hex = Integer.toHexString(0xff & byteBuffer[i]);
                    if (hex.length() == 1) {
                        strHexString.append('0');
                    }
                    strHexString.append(hex);
                }
                strResult = strHexString.toString();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return strResult;


    }

public static void main(String[] args) {
        String a = udf_lps_did.evaluate("142d27fe3b3d");

        //4d8797c903b46df1b48a5df93d5598a1b333f5dc4fbfb7258cc739e57994e63c
        System.out.println(a);
    }

}



