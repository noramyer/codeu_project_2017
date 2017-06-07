package codeu.chat.database;

import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.auth.FirebaseCredentials;
import java.io.FileInputStream;
import java.io.IOException;

public class DatabaseAccess {

  private final String account = "./third_party/codeu-56fe6-firebase-adminsdk-6et4u-e78898c410.json";
  private final String databaseUrl = "https://codeu-56fe6.firebaseio.com";

  public void initialize() {
    try {
      FileInputStream serviceAccount =
          new FileInputStream(account);

      FirebaseOptions options = new FirebaseOptions.Builder()
          .setCredential(FirebaseCredentials.fromCertificate(serviceAccount))
          .setDatabaseUrl(databaseUrl)
          .build();

      FirebaseApp.initializeApp(options);
    } catch (IOException e) {
      System.out.println(e.toString());
    }
  }
}