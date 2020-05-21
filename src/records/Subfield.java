package records;

import lombok.Data;

import java.util.List;

@Data
public class Subfield {
  /**
   * the name of this subfield
   */
  private char name;
  /**
   * subfield content; an empty string if the subfield is empty
   */
  private String content;
  /**
   * the list of subsubfields
   */
  private List<Subsubfield> subsubfields;
  /**
   * a secondary field contained by this subfield
   */
  private Field secField;
}