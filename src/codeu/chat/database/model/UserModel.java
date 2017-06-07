package codeu.chat.database.model;

/**
 * Created by greg on 6/1/17.
 */
public class UserModel {

  public String id;
  public String name;
  public String creation;

  public UserModel(String id, String name, String creation) {
    this.id = id;
    this.name = name;
    this.creation = creation;
  }

  // Default constructor needs to be explicitly defined for FireBase
  public UserModel() {
  }
}
