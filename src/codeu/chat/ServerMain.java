// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package codeu.chat;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import codeu.chat.common.Hub;
import codeu.chat.common.Relay;
import codeu.chat.common.Secret;
import codeu.chat.common.Uuid;
import codeu.chat.common.Uuids;
import codeu.chat.server.NoOpRelay;
import codeu.chat.server.RemoteRelay;
import codeu.chat.server.Server;
import codeu.chat.util.Logger;
import codeu.chat.util.RemoteAddress;
import codeu.chat.util.connections.ClientConnectionSource;
import codeu.chat.util.connections.Connection;
import codeu.chat.util.connections.ConnectionSource;
import codeu.chat.util.connections.ServerConnectionSource;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.auth.FirebaseCredentials;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

final class ServerMain {

  private static final Logger.Log LOG = Logger.newLog(ServerMain.class);

  public static void main(String[] args) {


    /*

    Just some sample code to test firebase from the server side.
    The following is just a test to get started

     */


    try {

      // todo Download the json file as instructed on firebase and save in file
      FileInputStream serviceAccount =
              new FileInputStream("testingjavafirebase-firebase-adminsdk-c1loq-3f4c316dc3 .json");


      // todo Change the databaseUrl
      FirebaseOptions options = new FirebaseOptions.Builder()
              .setCredential(FirebaseCredentials.fromCertificate(serviceAccount))
              .setDatabaseUrl("https://testingjavafirebase.firebaseio.com")
              .build();

      FirebaseApp.initializeApp(options);

    }
    catch (FileNotFoundException exc) {
      System.out.println("Could not find the json file");
    }


    final FirebaseDatabase database = FirebaseDatabase.getInstance();
    DatabaseReference ref = database.getReference();

    Map<String, Integer> myMap = new HashMap<>();
    myMap.put("test", 1);
    ref.setValue(myMap);


    /*

    ***** ORIGINAL CODE BELOW ******

     */


    Logger.enableConsoleOutput();

    try {
      Logger.enableFileOutput("chat_server_log.log");
    } catch (IOException ex) {
      LOG.error(ex, "Failed to set logger to write to file");
    }

    LOG.info("============================= START OF LOG =============================");

    final Uuid id = Uuids.fromString(args[0]);
    final byte[] secret = Secret.parse(args[1]);

    final int myPort = Integer.parseInt(args[2]);

    final RemoteAddress relayAddress = args.length > 3 ?
                                       RemoteAddress.parse(args[3]) :
                                       null;

    try (
        final ConnectionSource serverSource = ServerConnectionSource.forPort(myPort);
        final ConnectionSource relaySource = relayAddress == null ? null : new ClientConnectionSource(relayAddress.host, relayAddress.port)
    ) {

      LOG.info("Starting server...");
      runServer(id, secret, serverSource, relaySource);

    } catch (IOException ex) {

      LOG.error(ex, "Failed to establish connections");

    }
  }

  private static void runServer(Uuid id,
                                byte[] secret,
                                ConnectionSource serverSource,
                                ConnectionSource relaySource) {

    final Relay relay = relaySource == null ?
                        new NoOpRelay() :
                        new RemoteRelay(relaySource);

    final Server server = new Server(id, secret, relay);

    LOG.info("Server object created.");

    final Runnable hub = new Hub(serverSource, new Hub.Handler() {

      @Override
      public void handle(Connection connection) throws Exception {

        server.handleConnection(connection);

      }

      @Override
      public void onException(Exception ex) {

        System.out.println("ERROR: Exception during server tick. Check log for details.");
        LOG.error(ex, "Exception during server tick.");

      }
    });

    LOG.info("Starting hub...");

    hub.run();

    LOG.info("Hub exited.");
  }
}
