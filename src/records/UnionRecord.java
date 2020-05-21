package records;

import lombok.Data;

import java.util.List;

@Data
public class UnionRecord extends Record {
  private String cameFrom;
  private List<String> duplicates;

  public UnionRecord(Record record){

  }
}
