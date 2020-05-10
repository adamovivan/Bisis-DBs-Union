package records;

import lombok.Data;

@Data
public class Author {
  /** the librarian username */
  private String username;
  /** the name of the librarian's institution */
  private String institution;
}
