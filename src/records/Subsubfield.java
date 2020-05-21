package records;

import lombok.Data;

@Data
public class Subsubfield {
  /**
   * subsubfield name
   */
  private char name;
  /**
   * subsubfield content
   */
  private String content;
}