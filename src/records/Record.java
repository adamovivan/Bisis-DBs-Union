package records;


import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

import static util.Constants.*;

import java.util.Date;
import java.util.List;

@Data
public class Record {
  private String cameFrom;

  @BsonProperty("duplicates")
  private List<String> duplicates;
  /**
   * record identifier
   */
  private int recordID;
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
        for (Subfield subfield : field.getSubfields()) {
          if (subfield.getName() == _a) {
            return subfield.getContent();
          }
        }
      }
    }
    return null;
  }
}
