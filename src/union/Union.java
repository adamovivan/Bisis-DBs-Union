package union;

import records.Duplicate;
import records.Field;
import records.Record;
import records.SubField;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Union {

  private Union() {}

  public static void mergeRecords(Record mainRecord, Record secondaryRecord) {

    if (mainRecord == null || secondaryRecord == null) {
      return;
    }

    mergeFields(mainRecord, secondaryRecord);
  }

  private static void mergeFields(Record mainRecord, Record secondaryRecord) {
    List<Field> mainRecFields = mainRecord.getFields();

    Set<String> fieldsSet = new HashSet<>();    // field names

    // main record fields
    for (Field mainField: mainRecFields) {
      fieldsSet.add(mainField.getName());

      Field secField = getField(secondaryRecord.getFields(), mainField.getName());

      if (secField == null) {   // field doesn't exist in secondary record
        continue;
      }

      List<SubField> mainSubFields = mainField.getSubfields();
      Set<Character> subfieldsSet = new HashSet<>();    // subfield names

      for (SubField mainSubField: mainSubFields) {
        subfieldsSet.add(mainSubField.getName());
      }
      // adds subfields from secondary field that does not exist in main field
      for (SubField secSubField: secField.getSubfields()) {
        if (!subfieldsSet.contains(secSubField.getName())){
          mainSubFields.add(secSubField);
        }
      }
    }

    // secondary record fields, adds fields that does not exist in main record
    for (Field secField: secondaryRecord.getFields()) {
      if (!fieldsSet.contains(secField.getName())) {
        mainRecFields.add(secField);
      }
    }
  }

  private static Field getField(List<Field> fields, String name) {
    for (Field field: fields) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    return null;
  }

  public static void setDefaultMetadata(Record record) {
    record.setCreationDate(new Date());
    record.setLastModifiedDate(new Date());
    record.setModifier(null);
    record.setCreator(null);
  }

  public static void setUpdateMetadata(Record record) {
    record.setLastModifiedDate(new Date());
  }

  public static void addDuplicate(Record record, String duplicateName, int duplicateRecordId) {
    if (record.getDuplicates() == null) {
      record.setDuplicates(Collections.singletonList(new Duplicate(duplicateName, duplicateRecordId)));
    } else {
      record.getDuplicates().add(new Duplicate(duplicateName, duplicateRecordId));
    }
  }

  public static void updateDuplicate(Record record, String duplicateName, int duplicateRecordId) {
    if (record.getDuplicates() == null) {
      record.setDuplicates(Collections.singletonList(new Duplicate(duplicateName, duplicateRecordId)));
    } else if (!containsDuplicate(record.getDuplicates(), duplicateName)) {
      record.getDuplicates().add(new Duplicate(duplicateName, duplicateRecordId));
    }
  }

  private static boolean containsDuplicate(List<Duplicate> duplicates, String dbName) {
    return duplicates.stream().anyMatch(duplicate -> duplicate.getName().equals(dbName));
  }
}
