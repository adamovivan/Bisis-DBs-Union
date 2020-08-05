package union;

import org.bson.conversions.Bson;
import records.Duplicate;
import records.Field;
import records.Record;
import records.SubField;
import records.SubSubField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Updates.set;
import static util.Constants.CREATION_DATE;
import static util.Constants.CREATOR;
import static util.Constants.DUPLICATES;
import static util.Constants.FIELDS;
import static util.Constants.LAST_MODIFIED_DATE;
import static util.Constants.MODIFIER;

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
  }

  public static void addDuplicate(Record record, String duplicateName, int duplicateRn) {
    if (record.getDuplicates() == null) {
      record.setDuplicates(Collections.singletonList(new Duplicate(duplicateName, duplicateRn)));
    } else {
      record.getDuplicates().add(new Duplicate(duplicateName, duplicateRn));
    }
  }

  public static List<Bson> getUpdates(Record unionRecord) {
    List<Bson> updates = new ArrayList<>();
    updates.add(set(DUPLICATES, unionRecord.getDuplicates()));
    updates.add(set(FIELDS, unionRecord.getFields()));
    updates.add(set(CREATOR, unionRecord.getCreator()));
    updates.add(set(CREATION_DATE, unionRecord.getCreationDate()));
    updates.add(set(MODIFIER, unionRecord.getModifier()));
    updates.add(set(LAST_MODIFIED_DATE, unionRecord.getLastModifiedDate()));
    return updates;
  }

  private static SubField getSubField(List<SubField> subFields, char name) {
    for (SubField subField: subFields) {
      if (subField.getName() == name) {
        return subField;
      }
    }
    return null;
  }

  private static SubSubField getSubSubField(List<SubSubField> subSubFields, char name) {
    for (SubSubField subSubField: subSubFields) {
      if (subSubField.getName() == name) {
        return subSubField;
      }
    }
    return null;
  }
}
