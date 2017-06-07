package codeu.chat.server;

import codeu.chat.common.ConversationSummary;
import codeu.chat.common.Message;
import codeu.chat.common.NetworkCode;
import codeu.chat.common.User;
import codeu.chat.util.Serializers;
import codeu.chat.util.Uuid;
import codeu.chat.util.connections.Connection;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by rsharif on 3/17/17.
 */


public class BroadCastSystem {

  private class ConversationMessageLink {

    Uuid conversationUuid;
    User authorName;
    Message message;

    ConversationMessageLink(Uuid conversationUuid, User name, Message message) {
      this.conversationUuid = conversationUuid;
      this.authorName = name;
      this.message = message;
    }

  }

  private ConcurrentHashMap<Uuid, Set<Connection>> conversationUsers;
  private final BlockingQueue<ConversationMessageLink> messagesToBroadcast;
  private final Thread broadCaster;

  private class MessageBroadCaster implements Runnable {

    @Override
    public void run() {

      while (true) {
        try {
          broadCastMessage(messagesToBroadcast.take());
        } catch (InterruptedException exc) {
          System.out.println("Queue Interrupted");
        }
      }


    }
  }

  public BroadCastSystem() {

    conversationUsers = new ConcurrentHashMap<Uuid, Set<Connection>>();
    messagesToBroadcast = new LinkedBlockingQueue<ConversationMessageLink>();

    MessageBroadCaster messageBroadCaster = new MessageBroadCaster();

    broadCaster = new Thread(messageBroadCaster);

    broadCaster.start();
  }

  private void addConnection(Connection connection, Uuid uuid) {

    if (connection == null) {
      throw new NullPointerException();
    }
    Set<Connection> recipients = conversationUsers.get(uuid);
    recipients.add(connection);

  }

  private void removeConnection(Connection connection, Uuid uuid) {

    if (connection == null) {
      throw new NullPointerException();
    }
    Set<Connection> recipients = conversationUsers.get(uuid);
    recipients.remove(connection);

  }

  public void switchConversation(Connection connection, ConversationSummary oldCon,
      ConversationSummary newCon) {

    if (oldCon != null) {
      removeConnection(connection, oldCon.id);
    }
    if (newCon != null) {
      addConnection(connection, newCon.id);
    }

  }

  private void broadCastMessage(ConversationMessageLink messageLink) {

    Uuid conversationId = messageLink.conversationUuid;
    Message message = messageLink.message;
    User author = messageLink.authorName;

    // using an iterator in order to remove connections if they return an exception
    // this is in case the client has disconnected for whatever reason

    Iterator<Connection> myIterator = conversationUsers.get(conversationId).iterator();

    while (myIterator.hasNext()) {
      Connection connection = myIterator.next();

      try {

        OutputStream out = connection.out();

        synchronized (out) {

          PrintWriter writer = new PrintWriter(out, true);

          Serializers.INTEGER.write(writer, NetworkCode.NEW_BROADCAST);
          Uuid.SERIALIZER.write(writer, conversationId);
          User.SERIALIZER.write(writer, author);
          Message.SERIALIZER.write(writer, message);
        }

      } catch (Exception ex) {
        // todo (raami): consider adding a way to retry sending messages
        try {
          connection.close();
        } catch (IOException ioexc) {
          System.out.println("Error closing socket");
        }
        myIterator.remove();
      }

    }

  }

  public void addConversation(ConversationSummary conversationSummary) {

    if (conversationSummary == null) {
      throw new NullPointerException("Cannot add null to conversations");
    }
    if (conversationSummary.id == null) {
      throw new NullPointerException("Null ID in conversation summary");
    }

    Uuid uid = conversationSummary.id;

    if (conversationUsers.contains(uid)) {
      throw new IllegalArgumentException("Conversation already exists");
    }

    conversationUsers.put(uid, Collections.synchronizedSet(new HashSet<Connection>()));

  }

  // adds the given message to the list of messages that need to be broadcasted
  public void addMessage(Uuid conversationUuid, User author, Message message) {
    if (message == null) {
      throw new NullPointerException("Message cannot be null");
    }
    messagesToBroadcast.add(new ConversationMessageLink(conversationUuid, author, message));
  }

}