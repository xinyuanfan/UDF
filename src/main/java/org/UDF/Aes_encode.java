package org.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class Aes_encode extends UDF {
    public static String encrypt(String plainText, String key) throws Exception {
        // 创建AES加密器
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(), "AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);

        // 执行加密
        byte[] encryptedBytes = cipher.doFinal(plainText.getBytes("UTF-8"));
        return Base64.getEncoder().encodeToString(encryptedBytes);
    }
    public String evaluate(String str, String str1) {

        try {
                String phone = encrypt(str, str1);
                return phone;
        } catch (Exception e) {
            // 在这里处理异常，例如记录日志或返回默认值
            e.printStackTrace();
            return "Error: Unable to encrypt";
        }
    }
    public static void main(String[] args) {
        String key = "1ec56c326915845ca0174ab9e313bd79"; // 与Go加密代码中使用的密钥匹配
        String decryptedText = "13520287270";
        Aes_encode aes = new Aes_encode();
        String phone = aes.evaluate(decryptedText,key);
        System.out.println(phone.toString());
    }
}