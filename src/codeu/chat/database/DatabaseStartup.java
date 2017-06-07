package codeu.chat.database;

import codeu.chat.database.model.ConversationModel;
import codeu.chat.database.model.MessageModel;
import codeu.chat.database.model.UserModel;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DatabaseStartup {

  private final String USERS_CHILD = "users";
  private final String MESSAGES_CHILD = "messages";
  private final String CONVERSATIONS_CHILD = "conversations";

  // FireBase database reference for class
  private static DatabaseReference database = FirebaseDatabase.getInstance().getReference();

  /*
   *  Retrieves users from FireBase
   */
  public Map<String, UserModel> getUsers() {
    Map<String, UserModel> usersMap = new HashMap<>();
    DatabaseReference usersRef = database.child(USERS_CHILD);
    usersRef.addListenerForSingleValueEvent(new ValueEventListener() {
      @Override
      public void onDataChange(DataSnapshot dataSnapshot) {
        Iterator<DataSnapshot> usersIterator = dataSnapshot.getChildren().iterator();
        while (usersIterator.hasNext()) {
          UserModel user = usersIterator.next().getValue(UserModel.class);
          String key = user.id;
          usersMap.put(key, user);
        }
      }

      @Override
      public void onCancelled(DatabaseError databaseError) {
        System.out.println("The read failed: " + databaseError.getCode());
      }
    });
    return usersMap;
  }

  /*
   *  Retrieves conversations from FireBase
   */
  public Map<String, ConversationModel> getConversations() {
    Map<String, ConversationModel> convosMap = new HashMap<>();
    DatabaseReference convosRef = database.child(CONVERSATIONS_CHILD);
    convosRef.addListenerForSingleValueEvent(new ValueEventListener() {
      @Override
      public void onDataChange(DataSnapshot dataSnapshot) {
        Iterator<DataSnapshot> convosIterator = dataSnapshot.getChildren().iterator();
        while (convosIterator.hasNext()) {
          ConversationModel convo = convosIterator.next().getValue(ConversationModel.class);
          String key = convo.id;
          convosMap.put(key, convo);
        }
      }

      @Override
      public void onCancelled(DatabaseError databaseError) {
        System.out.println("The read failed: " + databaseError.getCode());
      }
    });
    return convosMap;
  }

  /*
*  Retrieves messages from FireBase
*/
  public Map<String, MessageModel> getMessages() {
    Map<String, MessageModel> messagesMap = new HashMap<>();
    DatabaseReference messagesRef = database.child(MESSAGES_CHILD);
    messagesRef.addListenerForSingleValueEvent(new ValueEventListener() {
      @Override
      public void onDataChange(DataSnapshot dataSnapshot) {
        Iterator<DataSnapshot> messagesIterator = dataSnapshot.getChildren().iterator();
        while (messagesIterator.hasNext()) {
          MessageModel message = messagesIterator.next().getValue(MessageModel.class);
          String key = message.id;
          messagesMap.put(key, message);
        }
      }

      @Override
      public void onCancelled(DatabaseError databaseError) {
        System.out.println("The read failed: " + databaseError.getCode());
      }
    });
    return messagesMap;
  }
}
