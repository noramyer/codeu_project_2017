package codeu.chat.client.simplegui2;

import codeu.chat.client.BroadCastReceiver;
import codeu.chat.client.ClientContext;
import codeu.chat.client.Controller;
import codeu.chat.client.View;
import codeu.chat.util.Logger;

public class MainGui {

  private final static Logger.Log LOG = Logger.newLog(MainGui.class);

  private final ClientContext clientContext;
  private final BroadCastReceiver receiver;

  //Constructor Method
  public MainGui(Controller controller, View view, BroadCastReceiver receiver) {
    clientContext = new ClientContext(controller, view);
    this.receiver = receiver;
  }

  public void start() {
    try {
      //create ChatMenu object and display stage
      ChatMenu mainFrame = new ChatMenu(clientContext, receiver);
      mainFrame.display();

    } catch (Exception ex) {
      System.out.println("ERROR: Exception in ChatSimpleGui.run. Check log for details.");
      LOG.error(ex, "Exception in ChatSimpleGui.run");
      System.exit(1);
    }

  }

}

