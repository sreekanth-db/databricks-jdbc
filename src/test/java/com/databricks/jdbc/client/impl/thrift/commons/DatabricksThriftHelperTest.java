package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DatabricksThriftHelperTest {
  @Test
  void testByteBufferToString() {
    long expectedLong = 123456789L;
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(expectedLong);
    buffer.flip();
    String result = DatabricksThriftHelper.byteBufferToString(buffer);
    String expectedUUID = new UUID(expectedLong, expectedLong).toString();
    assertEquals(expectedUUID, result);
  }

  @Test
  void testVerifySuccessStatus() {
    assertDoesNotThrow(
        () -> DatabricksThriftHelper.verifySuccessStatus(TStatusCode.SUCCESS_STATUS, "test"));
    assertDoesNotThrow(
        () ->
            DatabricksThriftHelper.verifySuccessStatus(
                TStatusCode.SUCCESS_WITH_INFO_STATUS, "test"));
    assertThrows(
        DatabricksHttpException.class,
        () -> DatabricksThriftHelper.verifySuccessStatus(TStatusCode.ERROR_STATUS, "test"));
  }

  private static Stream<Arguments> resultDataTypes() {
    TRowSet binaryRowSet =
        new TRowSet()
            .setColumns(
                Collections.singletonList(
                    TColumn.binaryVal(
                        new TBinaryColumn()
                            .setValues(
                                Collections.singletonList(
                                    ByteBuffer.wrap(TEST_STRING.getBytes()))))));
    TRowSet boolRowSet =
        new TRowSet()
            .setColumns(
                Collections.singletonList(
                    TColumn.boolVal(
                        new TBoolColumn().setValues(List.of(false, true, false, true)))));
    TRowSet byteRowSet =
        new TRowSet()
            .setColumns(
                Collections.singletonList(
                    TColumn.byteVal(new TByteColumn().setValues(List.of((byte) 5)))));
    TRowSet doubleRowSet =
        new TRowSet()
            .setColumns(
                Collections.singletonList(
                    TColumn.doubleVal(
                        new TDoubleColumn().setValues(List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)))));
    TRowSet i16RowSet =
        new TRowSet()
            .setColumns(
                Collections.singletonList(
                    TColumn.i16Val(new TI16Column().setValues(List.of((short) 1)))));
    TRowSet i32RowSet =
        new TRowSet()
            .setColumns(
                Collections.singletonList(TColumn.i32Val(new TI32Column().setValues(List.of(1)))));
    TRowSet i64RowSet =
        new TRowSet()
            .setColumns(
                Collections.singletonList(
                    TColumn.i64Val(new TI64Column().setValues(List.of(1L, 5L)))));
    return Stream.of(
        Arguments.of(new TRowSet(), 0),
        Arguments.of(new TRowSet().setColumns(Collections.emptyList()), 0),
        Arguments.of(boolRowSet, 4),
        Arguments.of(byteRowSet, 1),
        Arguments.of(doubleRowSet, 6),
        Arguments.of(i16RowSet, 1),
        Arguments.of(i32RowSet, 1),
        Arguments.of(i64RowSet, 2),
        Arguments.of(binaryRowSet, 1));
  }

  @ParameterizedTest
  @MethodSource("resultDataTypes")
  public void testRowCount(TRowSet resultData, int expectedRowCount) {
    assertEquals(expectedRowCount, DatabricksThriftHelper.getRowCount(resultData));
  }

  private static Stream<Arguments> manifestTypes() {
    return Stream.of(
        Arguments.of(null, 0),
        Arguments.of(new TGetResultSetMetadataResp(), 0),
        Arguments.of(
            new TGetResultSetMetadataResp()
                .setSchema(
                    new TTableSchema().setColumns(Collections.singletonList(new TColumnDesc()))),
            1));
  }

  @ParameterizedTest
  @MethodSource("manifestTypes")
  public void testColumnCount(TGetResultSetMetadataResp resultManifest, int expectedColumnCount) {
    assertEquals(expectedColumnCount, DatabricksThriftHelper.getColumnCount(resultManifest));
  }
}
