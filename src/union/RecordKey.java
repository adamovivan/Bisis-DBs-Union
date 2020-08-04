package union;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RecordKey {
  private Integer recordId;
  private String databaseName;
}