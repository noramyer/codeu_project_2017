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


package codeu.chat.server;

import codeu.chat.common.Conversation;
import codeu.chat.common.ConversationSummary;
import codeu.chat.common.Message;
import codeu.chat.common.NetworkCode;
import codeu.chat.common.Relay;
import codeu.chat.common.SentimentScore;
import codeu.chat.common.User;
import codeu.chat.util.Logger;
import codeu.chat.util.Serializers;
import codeu.chat.util.Time;
import codeu.chat.util.Timeline;
import codeu.chat.util.Uuid;
import codeu.chat.util.connections.Connection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

public final class Server {

  private static final Logger.Log LOG = Logger.newLog(Server.class);

  private static final int RELAY_REFRESH_MS = 5000;  // 5 seconds

  private final Timeline timeline = new Timeline();

  private final Uuid id;
  private final byte[] secret;

  private final Model model = new Model();
  private final View view = new View(model);
  private final Controller controller;

  private final Relay relay;
  private Uuid lastSeen = Uuid.NULL;
  private BroadCastSystem broadCastSystem = null;

  private class ConnectionListener implements Runnable {

    private Connection connection;

    public ConnectionListener(Connection connection) {
      this.connection = connection;
    }

    @Override
    public void run() {

      // Connection listener will always listen to this connection until an exception
      // is given off

      try {

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.in()));
        PrintWriter out = new PrintWriter(connection.out(), true);

        while (onMessage(connection, in, out)) {
          ;
        }

      } catch (IOException exc) {
        System.out.println("IOException in BroadCast System");
      }
      System.out.println("*********************Thread Exiting *****************");
    }

  }

  public Server(final Uuid id, final byte[] secret, final Relay relay) {

    this.id = id;
    this.secret = Arrays.copyOf(secret, secret.length);
    this.controller = new Controller(id, model);
    this.relay = relay;
    this.broadCastSystem = new BroadCastSystem();

    timeline.scheduleNow(new Runnable() {
      @Override
      public void run() {
        try {

          LOG.info("Reading update from relay...");

          for (final Relay.Bundle bundle : relay.read(id, secret, lastSeen, 32)) {
            onBundle(bundle);
            lastSeen = bundle.id();
          }

        } catch (Exception ex) {

          LOG.error(ex, "Failed to read update from relay.");

        }

        timeline.scheduleIn(RELAY_REFRESH_MS, this);
      }
    });
  }

  public void handleConnection(final Connection connection) {
    timeline.scheduleNow(new Runnable() {
      @Override
      public void run() {
        try {

          LOG.info("Handling connection...");

          ConnectionListener connectionListener = new ConnectionListener(connection);
          Thread connectionThread = new Thread(connectionListener);
          connectionThread.start();
        } catch (Exception ex) {

          LOG.error(ex, "Exception while handling connection.");

        }

      }
    });
  }

  private boolean onMessage(Connection connection, BufferedReader in, PrintWriter out)
      throws IOException {

    final int type = Serializers.INTEGER.read(in);

    // if the type is -1 the client has closed connection
    if (type == -1) {
      return false;
    }
    if (type == NetworkCode.NEW_MESSAGE_REQUEST) {

      final Uuid author = Uuid.SERIALIZER.read(in);
      final Uuid conversation = Uuid.SERIALIZER.read(in);
      final String content = Serializers.STRING.read(in);

      final Message message = controller.newMessage(author, conversation, content);
      final User user = view.findUser(author);
      user.sentimentScore.addMessage(message);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.NEW_MESSAGE_RESPONSE);
        Serializers.nullable(Message.SERIALIZER).write(out, message);
      }

      broadCastSystem.addMessage(conversation, user, message);

      timeline.scheduleNow(createSendToRelayEvent(
          author,
          conversation,
          message.id));

    } else if (type == NetworkCode.NEW_USER_REQUEST) {

      final String name = Serializers.STRING.read(in);

      final User user = controller.newUser(name);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.NEW_USER_RESPONSE);
        Serializers.nullable(User.SERIALIZER).write(out, user);
      }
    } else if (type == NetworkCode.NEW_CONVERSATION_REQUEST) {

      final String title = Serializers.STRING.read(in);
      final Uuid owner = Uuid.SERIALIZER.read(in);

      final Conversation conversation = controller.newConversation(title, owner);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.NEW_CONVERSATION_RESPONSE);
        Serializers.nullable(Conversation.SERIALIZER).write(out, conversation);
      }
      broadCastSystem.addConversation(conversation.summary);

    } else if (type == NetworkCode.GET_USERS_BY_ID_REQUEST) {

      final Collection<Uuid> ids = Serializers.collection(Uuid.SERIALIZER).read(in);

      final Collection<User> users = view.getUsers(ids);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_USERS_BY_ID_RESPONSE);
        Serializers.collection(User.SERIALIZER).write(out, users);
      }
    } else if (type == NetworkCode.GET_ALL_CONVERSATIONS_REQUEST) {

      final Collection<ConversationSummary> conversations = view.getAllConversations();

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_ALL_CONVERSATIONS_RESPONSE);
        Serializers.collection(ConversationSummary.SERIALIZER).write(out, conversations);
      }
    } else if (type == NetworkCode.GET_CONVERSATIONS_BY_ID_REQUEST) {

      final Collection<Uuid> ids = Serializers.collection(Uuid.SERIALIZER).read(in);

      final Collection<Conversation> conversations = view.getConversations(ids);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_CONVERSATIONS_BY_ID_RESPONSE);
        Serializers.collection(Conversation.SERIALIZER).write(out, conversations);
      }
    } else if (type == NetworkCode.GET_MESSAGES_BY_ID_REQUEST) {

      final Collection<Uuid> ids = Serializers.collection(Uuid.SERIALIZER).read(in);

      final Collection<Message> messages = view.getMessages(ids);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_MESSAGES_BY_ID_RESPONSE);
        Serializers.collection(Message.SERIALIZER).write(out, messages);
      }
    } else if (type == NetworkCode.GET_USER_GENERATION_REQUEST) {

      Serializers.INTEGER.write(out, NetworkCode.GET_USER_GENERATION_RESPONSE);
      Uuid.SERIALIZER.write(out, view.getUserGeneration());

    } else if (type == NetworkCode.GET_USERS_EXCLUDING_REQUEST) {

      final Collection<Uuid> ids = Serializers.collection(Uuid.SERIALIZER).read(in);

      final Collection<User> users = view.getUsersExcluding(ids);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_USERS_EXCLUDING_RESPONSE);
        Serializers.collection(User.SERIALIZER).write(out, users);
      }
    } else if (type == NetworkCode.GET_CONVERSATIONS_BY_TIME_REQUEST) {

      final Time startTime = Time.SERIALIZER.read(in);
      final Time endTime = Time.SERIALIZER.read(in);

      final Collection<Conversation> conversations = view.getConversations(startTime, endTime);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_CONVERSATIONS_BY_TIME_RESPONSE);
        Serializers.collection(Conversation.SERIALIZER).write(out, conversations);
      }
    } else if (type == NetworkCode.GET_CONVERSATIONS_BY_TITLE_REQUEST) {

      final String filter = Serializers.STRING.read(in);

      final Collection<Conversation> conversations = view.getConversations(filter);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_CONVERSATIONS_BY_TITLE_RESPONSE);
        Serializers.collection(Conversation.SERIALIZER).write(out, conversations);
      }
    } else if (type == NetworkCode.GET_MESSAGES_BY_TIME_REQUEST) {

      final Uuid conversation = Uuid.SERIALIZER.read(in);
      final Time startTime = Time.SERIALIZER.read(in);
      final Time endTime = Time.SERIALIZER.read(in);

      final Collection<Message> messages = view.getMessages(conversation, startTime, endTime);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_MESSAGES_BY_TIME_RESPONSE);
        Serializers.collection(Message.SERIALIZER).write(out, messages);
      }
    } else if (type == NetworkCode.GET_MESSAGES_BY_RANGE_REQUEST) {

      final Uuid rootMessage = Uuid.SERIALIZER.read(in);
      final int range = Serializers.INTEGER.read(in);

      final Collection<Message> messages = view.getMessages(rootMessage, range);

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_MESSAGES_BY_RANGE_RESPONSE);
        Serializers.collection(Message.SERIALIZER).write(out, messages);
      }

    } else if (type == NetworkCode.JOIN_CONVERSATION_REQUEST) {

      ConversationSummary old = Serializers.nullable(ConversationSummary.SERIALIZER).read(in);
      ConversationSummary newCon = Serializers.nullable(ConversationSummary.SERIALIZER).read(in);
      broadCastSystem.switchConversation(connection, old, newCon);
      // can send join conversation response
      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.JOIN_CONVERSATION_RESPONSE);
      }

    } else if (type == NetworkCode.GET_USER_SCORE_REQUEST) {

      final User user = User.SERIALIZER.read(in);
      final SentimentScore score = view.findUser(user.id).sentimentScore;

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.GET_USER_SCORE_RESPONSE);
        SentimentScore.SERIALIZER.write(out, score);
      }

    } else {

      // In the case that the message was not handled make a dummy message with
      // the type "NO_MESSAGE" so that the client still gets something.

      synchronized (connection.out()) {
        Serializers.INTEGER.write(out, NetworkCode.NO_MESSAGE);
      }
    }

    return true;
  }

  private void onBundle(Relay.Bundle bundle) {

    final Relay.Bundle.Component relayUser = bundle.user();
    final Relay.Bundle.Component relayConversation = bundle.conversation();
    final Relay.Bundle.Component relayMessage = bundle.user();

    User user = model.userById().first(relayUser.id());

    if (user == null) {
      user = controller.newUser(relayUser.id(), relayUser.text(), relayUser.time());
    }

    Conversation conversation = model.conversationById().first(relayConversation.id());

    if (conversation == null) {

      // As the relay does not tell us who made the conversation - the first person who
      // has a message in the conversation will get ownership over this server's copy
      // of the conversation.
      conversation = controller.newConversation(relayConversation.id(),
          relayConversation.text(),
          user.id,
          relayConversation.time());
    }

    Message message = model.messageById().first(relayMessage.id());

    if (message == null) {
      message = controller.newMessage(relayMessage.id(),
          user.id,
          conversation.id,
          relayMessage.text(),
          relayMessage.time());
    }
  }

  private Runnable createSendToRelayEvent(final Uuid userId,
      final Uuid conversationId,
      final Uuid messageId) {
    return new Runnable() {
      @Override
      public void run() {
        final User user = view.findUser(userId);
        final Conversation conversation = view.findConversation(conversationId);
        final Message message = view.findMessage(messageId);
        relay.write(id,
            secret,
            relay.pack(user.id, user.name, user.creation),
            relay.pack(conversation.id, conversation.title, conversation.creation),
            relay.pack(message.id, message.content, message.creation));
      }
    };
  }
}
