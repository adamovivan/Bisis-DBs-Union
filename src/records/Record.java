package records;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import static util.Constants.*;

import java.util.Date;
import java.util.List;

@Data
public class Record {

  private String cameFrom;
  /**
   * record dbs duplicates
   */
  private List<Duplicate> duplicates;
  /**
   * record identifier
   */
  private int recordID;
  /**
   * record identifier from origin
   */
  private int originRecordID;
  /**
   * id in books_common collection
   */
  private Integer commonBookUid;
  /**
   * publication type
   */
  private int pubType;
  /**
   * the list of fields
   */
  private List<Field> fields;
  /**
   * record creator
   */
  private Author creator;
  /**
   * record modifier
   */
  private Author modifier;
  /**
   * record creation date
   */
  private Date creationDate;
  /**
   * last modification date
   */
  private Date lastModifiedDate;
  /**
   * list of modifications
   */
  private List<RecordModification> recordModifications;
  /**
   * if record is being edited by someone in this moment
   */
  private String inUseBy;
  /**
   * rn
   */
  private int rn;
  /**
   * user ratings collection of current record
   */
  private List<RecordRating> recordRatings;

  @BsonIgnore
  public String getISBN() {
    for (Field field : fields) {
      if (field.getName().equals(_010)) {
        for (SubField subfield : field.getSubfields()) {
          if (subfield.getName() == _a) {
            return subfield.getContent();
          }
        }
      }
    }
    return null;
  }

  @BsonIgnore
  public String getISSN() {
    for (Field field : fields) {
      if (field.getName().equals(_011)) {
        for (SubField subfield : field.getSubfields()) {
          if (subfield.getName() == _a) {
            return subfield.getContent();
          }
        }
      }
    }
    return null;
  }

  @BsonIgnore
  public String getTitle() {
    for (Field field : fields) {
      if (field.getName().equals(_200)) {
        for (SubField subfield : field.getSubfields()) {
          if (subfield.getName() == _a) {
            return subfield.getContent();
          }
        }
      }
    }
    return null;
  }

  @BsonIgnore
  public String getReleaseYear() {
    for (Field field : fields) {
      if (field.getName().equals(_210)) {
        for (SubField subfield : field.getSubfields()) {
          if (subfield.getName() == _d) {
            return subfield.getContent();
          }
        }
      }
    }
    return null;
  }

  @BsonIgnore
  public String getPublisher() {
    for (Field field : fields) {
      if (field.getName().equals(_210)) {
        for (SubField subfield : field.getSubfields()) {
          if (subfield.getName() == _c) {
            return subfield.getContent();
          }
        }
      }
    }
    return null;
  }

  @BsonIgnore
  public String getAuthor() {
    for (Field field : fields) {
      if (field.getName().equals(_700)) {
        String lastName = getAuthorsLastName(field.getSubfields());
        String firstName = getAuthorsFirstName(field.getSubfields());
        return lastName + " " + firstName;
      }
    }
    return null;
  }

  @BsonIgnore
  private String getAuthorsLastName(List<SubField> subFields) {
    for (SubField subfield : subFields) {
      if (subfield.getName() == _a) {
        return subfield.getContent();
      }
    }
    return null;
  }

  @BsonIgnore
  private String getAuthorsFirstName(List<SubField> subFields) {
    for (SubField subfield : subFields) {
      if (subfield.getName() == _b) {
        return subfield.getContent();
      }
    }
    return null;
  }

}
