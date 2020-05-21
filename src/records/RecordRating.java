package records;

import lombok.Data;

@Data
public class RecordRating {
  private String username;
  private String libraryMemberId;
  private Integer givenRating;
}
