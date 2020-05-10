package records;

import lombok.Data;

import java.util.List;

@Data
public class Field {
  /** the field name */
  private String name;
  /** the value of the first indicator */
  private char ind1;
  /** the value of the second indicator */
  private char ind2;
  /** the list of subfields */
  private List<Subfield> subfields;
}