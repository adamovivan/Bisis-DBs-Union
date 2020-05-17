package records;

import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class Record {
//    private String _id;
    /** record identifier */
    private int recordID;
    /** id in books_common collection */
    private Integer commonBookUid;
    /** publication type */
    private int pubType;
    /** the list of fields */
    private List<Field> fields;
    /** record creator */
    private Author creator;
    /** record modifier */
    private Author modifier;
    /** record creation date */
    private Date creationDate;
    /** last modification date */
    private Date lastModifiedDate;
    /** list of modifications */
    private List<RecordModification> recordModifications;
    /** if record is being edited by someone in this moment*/
    private String inUseBy;
    /** rn */
    private int rn;
    /** user ratings collection of current record */
    private List<RecordRating> recordRatings;
}
