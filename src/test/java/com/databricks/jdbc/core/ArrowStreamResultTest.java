package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;

@ExtendWith(MockitoExtension.class)
public class ArrowStreamResultTest {
    private ArrayList<ArrowResultChunk> resultChunks = new ArrayList<>();

    private List<ChunkInfo> chunkInfos = new ArrayList<>();

    private int numberOfChunks = 10;
    private Random random = new Random();

    private long rowsInChunk = 110L;

    private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
    private static final String CHUNK_URL_PREFIX = "chunk.databricks.com/";

    @Mock
    StatementExecutionService statementExecutionService;

    /*
    If running into Arrow memory buffer error, run with jvm argument --add-opens java.base/java.nio=ALL-UNNAMED
    i.e. mvn test -DargLine="--add-opens java.base/java.nio=ALL-UNNAMED"
     */
    @Test
    public void testIteration() throws Exception {
        // Arrange
        setupChunks();
        ResultManifest resultManifest = new ResultManifest()
                .setTotalChunkCount((long) this.numberOfChunks)
                .setTotalRowCount(this.numberOfChunks * 110L)
                .setTotalByteCount(1000L)
                .setChunks(this.chunkInfos)
                .setSchema(new ResultSchema().setColumns(new ArrayList<>()));

        ResultData resultData = new ResultData()
                .setExternalLinks(getChunkLinks(0L, false));

        IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
        DatabricksSession session = new DatabricksSession(connectionContext,
                new DatabricksSdkClient(connectionContext, statementExecutionService));

        MockedConstruction<ChunkDownloader> mocked = Mockito.mockConstruction(ChunkDownloader.class, (mock, context) -> {
            Mockito.when(mock.getChunk(anyLong())).thenAnswer(new Answer<ArrowResultChunk>() {
                @Override
                public ArrowResultChunk answer(InvocationOnMock invocation) {
                    long index = invocation.getArgument(0);
                    return resultChunks.get((int) index);
                }
            });
        });
        ArrowStreamResult result = new ArrowStreamResult(resultManifest, resultData, "statement_id", session);

        // Act & Assert
        for(int i = 0; i < this.numberOfChunks; ++i) {
            for(int j = 0; j < this.rowsInChunk; ++j) {
                assertTrue(result.hasNext());
                assertTrue(result.next());
            }
        }
        assertFalse(result.hasNext());
        assertFalse(result.next());
    }

    private void setupChunks() throws Exception {
        for (int i = 0; i < this.numberOfChunks; ++i) {
            ChunkInfo chunkInfo = new ChunkInfo()
                    .setChunkIndex((long) i)
                    .setByteCount(1000L)
                    .setRowOffset((long) (i * 110L))
                    .setRowCount(this.rowsInChunk);
            this.chunkInfos.add(chunkInfo);
            ArrowResultChunk arrowResultChunk = new ArrowResultChunk(chunkInfo, new RootAllocator(Integer.MAX_VALUE));
            Schema schema = createTestSchema();
            Object[][] testData = createTestData(schema, (int) this.rowsInChunk);
            File arrowFile = createTestArrowFile("TestFile", schema, testData, new RootAllocator(Integer.MAX_VALUE));
            arrowResultChunk.getArrowDataFromInputStream(new FileInputStream(arrowFile));
            this.resultChunks.add(arrowResultChunk);
        }
    }

    private File createTestArrowFile(String fileName, Schema schema, Object[][] testData, RootAllocator allocator) throws IOException {
        int rowsInRecordBatch = 20;
        File file = new File(fileName);
        int cols = testData.length;
        int rows = testData[0].length;
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        ArrowWriter writer = new ArrowStreamWriter(vectorSchemaRoot, new DictionaryProvider.MapDictionaryProvider(), new FileOutputStream(file));
        writer.start();
        for(int j = 0; j < rows; j += rowsInRecordBatch) {
            int rowsToAddToRecordBatch = min(rowsInRecordBatch, rows - j);
            vectorSchemaRoot.setRowCount(rowsToAddToRecordBatch);
            for(int i = 0; i < cols; i++) {
                Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
                FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(i);
                if(type.equals(Types.MinorType.INT)) {
                    IntVector intVector = (IntVector) fieldVector;
                    intVector.setInitialCapacity(rowsToAddToRecordBatch);
                    for(int k = 1; k < rowsToAddToRecordBatch; k++) {
                        intVector.set(k, 1, (int) testData[i][j + k]);
                    }
                }
                else if(type.equals(Types.MinorType.FLOAT8)) {
                    Float8Vector float8Vector = (Float8Vector) fieldVector;
                    float8Vector.setInitialCapacity(rowsToAddToRecordBatch);
                    for(int k = 1; k < rowsToAddToRecordBatch; k++) {
                        float8Vector.set(k, 1, (double) testData[i][j + k]);
                    }
                }
                fieldVector.setValueCount(rowsToAddToRecordBatch);
            }
            writer.writeBatch();
        }
        return file;
    }

    private Schema createTestSchema() {
        List<Field> fieldList = new ArrayList<>();
        FieldType fieldType1 = new FieldType(false, Types.MinorType.INT.getType(), null);
        FieldType fieldType2 = new FieldType(false, Types.MinorType.FLOAT8.getType(), null);
        fieldList.add(new Field("Field1", fieldType1, null));
        fieldList.add(new Field("Field2", fieldType2, null));
        return new Schema(fieldList);
    }

    private Object[][] createTestData(Schema schema, int rows) {
        int cols = schema.getFields().size();
        Object[][] data = new Object[cols][rows];
        for(int i = 0; i < cols; i++) {
            Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
            if(type.equals(Types.MinorType.INT)) {
                for(int j = 0; j < rows; j++) {
                    data[i][j] = random.nextInt();
                }
            }
            else if(type.equals(Types.MinorType.FLOAT8)) {
                for(int j = 0; j < rows; j++) {
                    data[i][j] = random.nextDouble();
                }
            }
        }
        return data;
    }

    private GetStatementResultChunkNRequest getChunkNRequest(long chunkIndex) {
        return new GetStatementResultChunkNRequest()
                .setStatementId("statement_id")
                .setChunkIndex(chunkIndex);
    }

    private List<ExternalLink> getChunkLinks(long chunkIndex, boolean isLast) {
        List<ExternalLink> chunkLinks = new ArrayList<>();
        ExternalLink chunkLink = new ExternalLink()
                .setChunkIndex(chunkIndex)
                .setExternalLink(CHUNK_URL_PREFIX + chunkIndex)
                .setExpiration(Instant.now().plusSeconds(3600L).toString());
        if (!isLast) {
            chunkLink.setNextChunkIndex(chunkIndex + 1);
        }
        chunkLinks.add(chunkLink);
        return chunkLinks;
    }
}
