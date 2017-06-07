package codeu.chat.database.model;

/**
 * Created by greg on 6/1/17.
 */
public class ConversationModel {

  public String id;
  public String owner;
  public String title;
  public String creation;

  public ConversationModel(String id, String owner, String title, String creation) {
    this.id = id;
    this.owner = owner;
    this.title = title;
    this.creation = creation;
  }

  // Default constructor needs to be explicitly defined for FireBase
  public ConversationModel() {
  }
}
