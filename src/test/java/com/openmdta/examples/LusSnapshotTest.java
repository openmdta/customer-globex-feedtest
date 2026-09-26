package com.openmdta.examples;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openmdta.sdk.p_globex.Format;
import com.openmdta.sdk.p_globex.Response;
import com.openmdta.sdk.p_globex.sbe.gateway_protocol.FeedSnapshotHeaderEncoder;
import com.openmdta.sdk.p_globex.sbe.gateway_protocol.MarketDataMessageBatchEncoder;
import com.openmdta.sdk.p_globex.sbe.gateway_protocol.Phase;
import com.openmdta.sdk.p_globex.sbe.stream_0.BidAskEncoder;
import com.openmdta.sdk.p_globex.sbe.stream_0.DecimalEncodingEncoder;
import com.openmdta.sdk.p_globex.sbe.stream_0.QuoteLevelEncoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import org.junit.jupiter.api.Test;

class LusSnapshotTest {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final LusSnapshot dump = new LusSnapshot(new PrintStream(bytes, true, StandardCharsets.UTF_8));
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void preservesSnapshotBoundaryGapsAndUnsignedIds() throws Exception {
        dump.onResponse(header("LUS@lus"));
        var row = json.readTree(bytes.toString(StandardCharsets.UTF_8));
        assertEquals("snapshot", row.path("type").asText());
        assertEquals("LUS@lus", row.path("dataset").asText());
        assertEquals("18446744073709551614", row.path("throughMessageId").asText());
        assertEquals("5", row.path("gaps").get(0).path("afterMessageId").asText());
        assertEquals("9", row.path("gaps").get(0).path("throughMessageId").asText());
    }

    @Test
    void decodesPricesExactlyAndPreservesMissingValues() throws Exception {
        dump.onResponse(header("LUS@lus"));
        dump.onResponse(quote(false));
        var lines = bytes.toString(StandardCharsets.UTF_8).lines().toList();
        assertEquals(2, lines.size());
        var row = json.readTree(lines.get(1));
        assertEquals("test\"key", row.path("recordKey").asText());
        assertEquals("18446744073709551613", row.path("messageId").asText());
        assertEquals("123456.789", row.path("bid").path("price").asText());
        assertTrue(row.path("bid").path("size").isNull());
        assertTrue(row.path("ask").isNull());
        assertTrue(row.path("quoteCondition").isNull());
        assertEquals("1700000000123456", row.path("eventTimeMicros").asText());
        assertFalse(row.path("clear").asBoolean());
    }

    @Test
    void clearFieldsDoNotDecodeAnAbsentPayload() throws Exception {
        dump.onResponse(header("LUS@lus"));
        dump.onResponse(quote(true));
        var row = json.readTree(bytes.toString(StandardCharsets.UTF_8).lines().toList().get(1));
        assertTrue(row.path("clear").asBoolean());
        assertFalse(row.has("bid"));
        assertFalse(row.has("ask"));
    }

    @Test
    void rejectsDataWithoutHeaderWrongDatasetAndDuplicateHeaders() throws Exception {
        assertThrows(IOException.class, () -> dump.onResponse(quote(false)));
        assertThrows(IOException.class, () -> dump.onResponse(header("OTHER@source")));
        assertEquals(0, bytes.size());
        dump.onResponse(header("LUS@lus"));
        assertThrows(IOException.class, () -> dump.onResponse(header("LUS@lus")));
    }

    @Test
    void failsWhenStdoutCannotBeWritten() {
        var broken = new PrintStream(new java.io.OutputStream() {
            @Override public void write(int value) throws IOException {
                throw new IOException("closed pipe");
            }
        });
        assertThrows(IOException.class, () -> new LusSnapshot(broken).onResponse(header("LUS@lus")));
    }

    private static Response header(String dataset) {
        var encoder = new FeedSnapshotHeaderEncoder().wrap(new UnsafeBuffer(new byte[256]), 0)
                .throughMessageId(-2L);
        encoder.gapsCount(1).next().afterMessageId(5).throughMessageId(9);
        byte[] name = dataset.getBytes(StandardCharsets.UTF_8);
        encoder.putDataset(name, 0, name.length);
        return response(encoder);
    }

    private static Response quote(boolean clear) {
        var payload = new UnsafeBuffer(new byte[BidAskEncoder.BLOCK_LENGTH]);
        var quote = new BidAskEncoder().wrap(payload, 0);
        quote.bid().price().mantissa(123456789).exponent((byte) -3);
        quote.bid().size(QuoteLevelEncoder.sizeNullValue());
        quote.ask().price().mantissa(DecimalEncodingEncoder.mantissaNullValue()).exponent((byte) 0);
        quote.ask().size(0);
        quote.quoteCondition(BidAskEncoder.quoteConditionNullValue());
        var encoder = new MarketDataMessageBatchEncoder().wrap(new UnsafeBuffer(new byte[512]), 0)
                .phase(Phase.SNAPSHOT);
        encoder.messagesCount(1).next().messageId(-3L).firstField(0).fieldCount(1);
        encoder.fieldsCount(1).next().schemaId(quote.sbeSchemaId()).templateId(quote.sbeTemplateId())
                .version(quote.sbeSchemaVersion()).blockLength(quote.sbeBlockLength())
                .eventTimeMicros(1700000000123456L).clear((short) (clear ? 1 : 0))
                .payloadOffset(0).payloadLength(clear ? 0 : payload.capacity());
        encoder.gapsCount(0);
        byte[] key = "test\"key".getBytes(StandardCharsets.UTF_8);
        byte[] dataset = "LUS@lus".getBytes(StandardCharsets.UTF_8);
        encoder.putDatasetRecordKey(key, 0, key.length).putDataset(dataset, 0, dataset.length)
                .putPayload(payload, 0, clear ? 0 : payload.capacity());
        return response(encoder);
    }

    private static Response response(MessageEncoderFlyweight encoder) {
        byte[] body = new byte[encoder.encodedLength()];
        encoder.buffer().getBytes(encoder.offset(), body);
        return new Response.Owned(new Format(encoder.sbeSchemaId(), encoder.sbeTemplateId(),
                encoder.sbeSchemaVersion(), encoder.sbeBlockLength()), body).view();
    }
}
