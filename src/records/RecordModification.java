package records;

import lombok.Data;

import java.util.Date;

@Data
public class RecordModification {
    /**Username of librarian*/
    private String librarianUsername;
    /**Date of modification*/
    private Date dateOfModification;
}
