/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestUpdateRequirementParser {

  @Test
  public void testUpdateRequirementWithoutRequirementTypeCannotParse() {
    List<String> invalidJson =
        ImmutableList.of(
            "{\"type\":null,\"uuid\":\"2cc52516-5e73-41f2-b139-545d41a4e151\"}",
            "{\"uuid\":\"2cc52516-5e73-41f2-b139-545d41a4e151\"}");

    for (String json : invalidJson) {
      assertThatThrownBy(() -> UpdateRequirementParser.fromJson(json))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cannot parse update requirement. Missing field: type");
    }
  }

  @Test
  public void testAssertUUIDFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_TABLE_UUID;
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String json = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement expected = new UpdateRequirement.AssertTableUUID(uuid);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertUUIDToJson() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String expected = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement actual = new UpdateRequirement.AssertTableUUID(uuid);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertTableUUID should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertViewUUIDFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_VIEW_UUID;
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String json = String.format("{\"type\":\"assert-view-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement expected = new UpdateRequirement.AssertViewUUID(uuid);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertViewUUIDToJson() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String expected = String.format("{\"type\":\"assert-view-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement actual = new UpdateRequirement.AssertViewUUID(uuid);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertViewUUID should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertTableDoesNotExistFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_TABLE_DOES_NOT_EXIST;
    String json = "{\"type\":\"assert-create\"}";
    UpdateRequirement expected = new UpdateRequirement.AssertTableDoesNotExist();
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertTableDoesNotExistToJson() {
    String expected = "{\"type\":\"assert-create\"}";
    UpdateRequirement actual = new UpdateRequirement.AssertTableDoesNotExist();
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertTableDoesNotExist should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertRefSnapshotIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID;
    String refName = "snapshot-name";
    Long snapshotId = 1L;
    String json =
        String.format(
            "{\"type\":\"%s\",\"ref\":\"%s\",\"snapshot-id\":%d}",
            requirementType, refName, snapshotId);
    UpdateRequirement expected = new UpdateRequirement.AssertRefSnapshotID(refName, snapshotId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertRefSnapshotIdToJsonWithNullSnapshotId() {
    String requirementType = UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID;
    String refName = "snapshot-name";
    Long snapshotId = null;
    String json =
        String.format(
            "{\"type\":\"%s\",\"ref\":\"%s\",\"snapshot-id\":%d}",
            requirementType, refName, snapshotId);
    UpdateRequirement expected = new UpdateRequirement.AssertRefSnapshotID(refName, snapshotId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertRefSnapshotIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID;
    String refName = "snapshot-name";
    Long snapshotId = 1L;
    String expected =
        String.format(
            "{\"type\":\"%s\",\"ref\":\"%s\",\"snapshot-id\":%d}",
            requirementType, refName, snapshotId);
    UpdateRequirement actual = new UpdateRequirement.AssertRefSnapshotID(refName, snapshotId);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertRefSnapshotId should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertRefSnapshotIdFromJsonWithNullSnapshotId() {
    String requirementType = UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID;
    String refName = "snapshot-name";
    Long snapshotId = null;
    String expected =
        String.format(
            "{\"type\":\"%s\",\"ref\":\"%s\",\"snapshot-id\":%d}",
            requirementType, refName, snapshotId);
    UpdateRequirement actual = new UpdateRequirement.AssertRefSnapshotID(refName, snapshotId);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertRefSnapshotId should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertLastAssignedFieldIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_FIELD_ID;
    int lastAssignedFieldId = 12;
    String json =
        String.format(
            "{\"type\":\"%s\",\"last-assigned-field-id\":%d}",
            requirementType, lastAssignedFieldId);
    UpdateRequirement expected =
        new UpdateRequirement.AssertLastAssignedFieldId(lastAssignedFieldId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertLastAssignedFieldIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_FIELD_ID;
    int lastAssignedFieldId = 12;
    String expected =
        String.format(
            "{\"type\":\"%s\",\"last-assigned-field-id\":%d}",
            requirementType, lastAssignedFieldId);
    UpdateRequirement actual = new UpdateRequirement.AssertLastAssignedFieldId(lastAssignedFieldId);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertLastAssignedFieldId should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertCurrentSchemaIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_CURRENT_SCHEMA_ID;
    int schemaId = 4;
    String json =
        String.format("{\"type\":\"%s\",\"current-schema-id\":%d}", requirementType, schemaId);
    UpdateRequirement expected = new UpdateRequirement.AssertCurrentSchemaID(schemaId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertCurrentSchemaIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_CURRENT_SCHEMA_ID;
    int schemaId = 4;
    String expected =
        String.format("{\"type\":\"%s\",\"current-schema-id\":%d}", requirementType, schemaId);
    UpdateRequirement actual = new UpdateRequirement.AssertCurrentSchemaID(schemaId);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertCurrentSchemaId should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertLastAssignedPartitionIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_PARTITION_ID;
    int lastAssignedPartitionId = 1004;
    String json =
        String.format(
            "{\"type\":\"%s\",\"last-assigned-partition-id\":%d}",
            requirementType, lastAssignedPartitionId);
    UpdateRequirement expected =
        new UpdateRequirement.AssertLastAssignedPartitionId(lastAssignedPartitionId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertLastAssignedPartitionIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_PARTITION_ID;
    int lastAssignedPartitionId = 1004;
    String expected =
        String.format(
            "{\"type\":\"%s\",\"last-assigned-partition-id\":%d}",
            requirementType, lastAssignedPartitionId);
    UpdateRequirement actual =
        new UpdateRequirement.AssertLastAssignedPartitionId(lastAssignedPartitionId);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertLastAssignedPartitionId should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertDefaultSpecIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_DEFAULT_SPEC_ID;
    int specId = 5;
    String json =
        String.format("{\"type\":\"%s\",\"default-spec-id\":%d}", requirementType, specId);
    UpdateRequirement expected = new UpdateRequirement.AssertDefaultSpecID(specId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertDefaultSpecIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_DEFAULT_SPEC_ID;
    int specId = 5;
    String expected =
        String.format("{\"type\":\"%s\",\"default-spec-id\":%d}", requirementType, specId);
    UpdateRequirement actual = new UpdateRequirement.AssertDefaultSpecID(specId);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertDefaultSpecId should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAssertDefaultSortOrderIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_DEFAULT_SORT_ORDER_ID;
    int sortOrderId = 10;
    String json =
        String.format(
            "{\"type\":\"%s\",\"default-sort-order-id\":%d}", requirementType, sortOrderId);
    UpdateRequirement expected = new UpdateRequirement.AssertDefaultSortOrderID(sortOrderId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertDefaultSortOrderIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_DEFAULT_SORT_ORDER_ID;
    int sortOrderId = 10;
    String expected =
        String.format(
            "{\"type\":\"%s\",\"default-sort-order-id\":%d}", requirementType, sortOrderId);
    UpdateRequirement actual = new UpdateRequirement.AssertDefaultSortOrderID(sortOrderId);
    assertThat(UpdateRequirementParser.toJson(actual))
        .as("AssertDefaultSortOrderId should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  public void assertEquals(
      String requirementType, UpdateRequirement expected, UpdateRequirement actual) {
    switch (requirementType) {
      case UpdateRequirementParser.ASSERT_TABLE_UUID:
        compareAssertTableUUID(
            (UpdateRequirement.AssertTableUUID) expected,
            (UpdateRequirement.AssertTableUUID) actual);
        break;
      case UpdateRequirementParser.ASSERT_VIEW_UUID:
        compareAssertViewUUID(
            (UpdateRequirement.AssertViewUUID) expected, (UpdateRequirement.AssertViewUUID) actual);
        break;
      case UpdateRequirementParser.ASSERT_TABLE_DOES_NOT_EXIST:
        // Don't cast here as the function explicitly tests that the types are correct, given that
        // the generated JSON
        // for ASSERT_TABLE_DOES_NOT_EXIST does not have any fields other than the requirement type.
        compareAssertTableDoesNotExist(expected, actual);
        break;
      case UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID:
        compareAssertRefSnapshotId(
            (UpdateRequirement.AssertRefSnapshotID) expected,
            (UpdateRequirement.AssertRefSnapshotID) actual);
        break;
      case UpdateRequirementParser.ASSERT_LAST_ASSIGNED_FIELD_ID:
        compareAssertLastAssignedFieldId(
            (UpdateRequirement.AssertLastAssignedFieldId) expected,
            (UpdateRequirement.AssertLastAssignedFieldId) actual);
        break;
      case UpdateRequirementParser.ASSERT_CURRENT_SCHEMA_ID:
        compareAssertCurrentSchemaId(
            (UpdateRequirement.AssertCurrentSchemaID) expected,
            (UpdateRequirement.AssertCurrentSchemaID) actual);
        break;
      case UpdateRequirementParser.ASSERT_LAST_ASSIGNED_PARTITION_ID:
        compareAssertLastAssignedPartitionId(
            (UpdateRequirement.AssertLastAssignedPartitionId) expected,
            (UpdateRequirement.AssertLastAssignedPartitionId) actual);
        break;
      case UpdateRequirementParser.ASSERT_DEFAULT_SPEC_ID:
        compareAssertDefaultSpecId(
            (UpdateRequirement.AssertDefaultSpecID) expected,
            (UpdateRequirement.AssertDefaultSpecID) actual);
        break;
      case UpdateRequirementParser.ASSERT_DEFAULT_SORT_ORDER_ID:
        compareAssertDefaultSortOrderId(
            (UpdateRequirement.AssertDefaultSortOrderID) expected,
            (UpdateRequirement.AssertDefaultSortOrderID) actual);
        break;
      default:
        fail("Unrecognized update requirement type: " + requirementType);
    }
  }

  private static void compareAssertTableUUID(
      UpdateRequirement.AssertTableUUID expected, UpdateRequirement.AssertTableUUID actual) {
    assertThat(actual.uuid())
        .as("UUID from JSON should not be null")
        .isNotNull()
        .as("UUID should parse correctly from JSON")
        .isEqualTo(expected.uuid());
  }

  private static void compareAssertViewUUID(
      UpdateRequirement.AssertViewUUID expected, UpdateRequirement.AssertViewUUID actual) {
    assertThat(actual.uuid())
        .as("UUID from JSON should not be null")
        .isNotNull()
        .as("UUID should parse correctly from JSON")
        .isEqualTo(expected.uuid());
  }

  // AssertTableDoesNotExist does not have any fields beyond the requirement type, so just check
  // that the classes
  // are the same and as expected.
  private static void compareAssertTableDoesNotExist(
      UpdateRequirement expected, UpdateRequirement actual) {
    assertThat(actual)
        .isOfAnyClassIn(UpdateRequirement.AssertTableDoesNotExist.class)
        .hasSameClassAs(expected);
  }

  private static void compareAssertRefSnapshotId(
      UpdateRequirement.AssertRefSnapshotID expected,
      UpdateRequirement.AssertRefSnapshotID actual) {
    assertThat(actual.refName())
        .as("Ref name should parse correctly from JSON")
        .isEqualTo(expected.refName());
    assertThat(actual.snapshotId())
        .as("Snapshot ID should parse correctly from JSON")
        .isEqualTo(expected.snapshotId());
  }

  private static void compareAssertLastAssignedFieldId(
      UpdateRequirement.AssertLastAssignedFieldId expected,
      UpdateRequirement.AssertLastAssignedFieldId actual) {
    assertThat(actual.lastAssignedFieldId())
        .as("Last assigned field id should parse correctly from JSON")
        .isEqualTo(expected.lastAssignedFieldId());
  }

  private static void compareAssertCurrentSchemaId(
      UpdateRequirement.AssertCurrentSchemaID expected,
      UpdateRequirement.AssertCurrentSchemaID actual) {
    assertThat(actual.schemaId())
        .as("Current schema id should parse correctly from JSON")
        .isEqualTo(expected.schemaId());
  }

  private static void compareAssertLastAssignedPartitionId(
      UpdateRequirement.AssertLastAssignedPartitionId expected,
      UpdateRequirement.AssertLastAssignedPartitionId actual) {
    assertThat(actual.lastAssignedPartitionId())
        .as("Last assigned partition id should parse correctly from JSON")
        .isEqualTo(expected.lastAssignedPartitionId());
  }

  private static void compareAssertDefaultSpecId(
      UpdateRequirement.AssertDefaultSpecID expected,
      UpdateRequirement.AssertDefaultSpecID actual) {
    assertThat(actual.specId())
        .as("Default spec id should parse correctly from JSON")
        .isEqualTo(expected.specId());
  }

  private static void compareAssertDefaultSortOrderId(
      UpdateRequirement.AssertDefaultSortOrderID expected,
      UpdateRequirement.AssertDefaultSortOrderID actual) {
    assertThat(actual.sortOrderId())
        .as("Default sort order id should parse correctly from JSON")
        .isEqualTo(expected.sortOrderId());
  }
}
