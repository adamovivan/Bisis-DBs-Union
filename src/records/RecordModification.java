package records;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class RecordModification {
  /**
   * Username of librarian
   */
  private String librarianUsername;
  /**
   * Date of modification
   */
  private LocalDateTime dateOfModification;
}
