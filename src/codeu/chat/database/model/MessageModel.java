package codeu.chat.database.model;

public class MessageModel {

  public String id;
  public String author;
  public String content;
  public String creation;

  public MessageModel(String author, String content, String creation) {
    this.author = author;
    this.content = content;
    this.creation = creation;
  }

  // Default constructor needs to be explicitly defined for FireBase
  public MessageModel() {
  }
}
