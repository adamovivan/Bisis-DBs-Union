package union;

import records.Duplicate;
import records.Field;
import records.Record;
import records.RecordModification;
import records.SubField;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
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
    List<Field> mainRecordFields = mainRecord.getFields();

    Set<String> fieldsSet = new HashSet<>();    // field names

    // main record fields
    for (Field mainField: mainRecordFields) {
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
      // adds subfields from secondary field which do not exist in main field
      for (SubField secSubField: secField.getSubfields()) {
        if (!subfieldsSet.contains(secSubField.getName())){
          mainSubFields.add(secSubField);
        }
      }
    }

    // secondary record fields, adds fields which do not exist in main record
    for (Field secField: secondaryRecord.getFields()) {
      if (!fieldsSet.contains(secField.getName())) {
        mainRecordFields.add(secField);
      }
    }
  }

//  private static void updateRecord(Record recordToUpdate, Record record, LocalDateTime lastUpdate) {
//    recordToUpdate.setCommonBookUid(record.getCommonBookUid());
//    recordToUpdate.setPubType(record.getPubType());
//    recordToUpdate.setFields(record.getFields());
//
//    List<RecordModification> recordModifications;
//
//    if (recordToUpdate.getRecordModifications() == null) {
//      recordModifications = new ArrayList<>();
//    } else {
//      recordModifications = recordToUpdate.getRecordModifications();
//    }
//
//    // update only newer modifications
//    record.getRecordModifications().forEach(recordModification -> {
//      if (recordModification.getDateOfModification().compareTo(lastUpdate) > 0) {
//        recordModifications.add(recordModification);
//      }
//    });
//
//    recordToUpdate.setRecordModifications(recordModifications);
//
//    recordToUpdate.setLastModifiedDate(LocalDateTime.now());
//    recordToUpdate.setInUseBy(record.getInUseBy());
//    re
//  }

  private static Field getField(List<Field> fields, String name) {
    for (Field field: fields) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    return null;
  }

  public static void setDefaultMetadata(Record record) {
    record.setCreationDate(LocalDateTime.now());
    record.setLastModifiedDate(LocalDateTime.now());
    record.setModifier(null);
    record.setCreator(null);
  }

  public static void setUpdateMetadata(Record record) {
    record.setLastModifiedDate(LocalDateTime.now());
  }

  public static void addDuplicate(Record record, String duplicateName, int duplicateRecordId) {
    if (record.getDuplicates() == null) {
      record.setDuplicates(Collections.singletonList(new Duplicate(duplicateName, duplicateRecordId)));
    } else {
      record.getDuplicates().add(new Duplicate(duplicateName, duplicateRecordId));
    }
  }

  public static void updateDuplicates(Record record, String duplicateName, int duplicateRecordId) {
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
