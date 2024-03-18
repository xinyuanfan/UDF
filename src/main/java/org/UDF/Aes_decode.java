package org.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class Aes_decode extends UDF{
    public static String decrypt(String encryptedText, String key) throws Exception {
        // 创建AES解密器
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(), "AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);

        // 执行解密
        byte[] encryptedBytes = Base64.getDecoder().decode(encryptedText);
        byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
        return new String(decryptedBytes, "UTF-8");
    }

    public String evaluate(String str, String str1) {

        try {
                String phone = decrypt(str, str1);
                return phone;
        } catch (Exception e) {
            // 在这里处理异常，例如记录日志或返回默认值
            e.printStackTrace();
            return "Error: Unable to decrypt";
        }
    }
    public static void main(String[] args) {

        String key = "1ec56c326915845ca0174ab9e313bd79"; // 与Go加密代码中使用的密钥匹配
        String encryptedText = "1R9rcekFdplpJpcuA9elTA==";


        Aes_decode aes = new Aes_decode();
       String phone = aes.evaluate(encryptedText,key);
        System.out.println(phone.toString());
    }
}