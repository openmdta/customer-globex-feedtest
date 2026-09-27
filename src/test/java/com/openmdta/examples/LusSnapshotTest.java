package com.openmdta.examples;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openmdta.sdk.p_globex.model.stream_0.BidAsk;
import com.openmdta.sdk.p_globex.model.stream_0.QuoteLevel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class LusSnapshotTest {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final LusSnapshot dump = new LusSnapshot(new PrintStream(bytes, true, StandardCharsets.UTF_8));
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void preservesSnapshotBoundaryGapsAndUnsignedIds() throws Exception {
        dump.onSnapshotBegin(-2L);
        dump.onSnapshotGap(5, 9);
        dump.onSnapshotGap(Long.MAX_VALUE, -2L);
        dump.onSnapshotComplete();
        var row = json.readTree(bytes.toString(StandardCharsets.UTF_8));
        assertEquals("snapshot", row.path("type").asText());
        assertEquals("LUS@lus", row.path("dataset").asText());
        assertEquals("DL", row.path("quality").asText());
        assertEquals("18446744073709551614", row.path("throughMessageId").asText());
        assertEquals(2, row.path("gaps").size());
        assertEquals("5", row.path("gaps").get(0).path("afterMessageId").asText());
        assertEquals("9", row.path("gaps").get(0).path("throughMessageId").asText());
        assertEquals("18446744073709551614", row.path("gaps").get(1).path("throughMessageId").asText());
    }

    @Test
    void preservesExactPricesMissingValuesAndHeaderOrdering() throws Exception {
        dump.onSnapshotBegin(-2L);
        dump.onSnapshotGap(5, 9);
        var quote = new BidAsk(Optional.of(new QuoteLevel(new BigDecimal("123456.789"), 100)),
                Optional.empty(), OptionalInt.empty());
        dump.writeQuote("test\"key", -3L, 1700000000123456L, Optional.of(quote));
        dump.onSnapshotComplete();
        var lines = bytes.toString(StandardCharsets.UTF_8).lines().toList();
        assertEquals(2, lines.size());
        assertEquals("snapshot", json.readTree(lines.get(0)).path("type").asText());
        assertEquals(1, json.readTree(lines.get(0)).path("gaps").size());
        var row = json.readTree(lines.get(1));
        assertEquals("test\"key", row.path("recordKey").asText());
        assertEquals("18446744073709551613", row.path("messageId").asText());
        assertEquals("123456.789", row.path("bid").path("price").asText());
        assertEquals(100, row.path("bid").path("size").asLong());
        assertTrue(row.path("ask").isNull());
        assertTrue(row.path("quoteCondition").isNull());
        assertEquals("1700000000123456", row.path("eventTimeMicros").asText());
        assertFalse(row.path("clear").asBoolean());
    }

    @Test
    void clearRecordsHaveNoQuoteValues() throws Exception {
        dump.onSnapshotBegin(10);
        dump.writeQuote("record", 10, 20, Optional.empty());
        var row = json.readTree(bytes.toString(StandardCharsets.UTF_8).lines().toList().get(1));
        assertTrue(row.path("clear").asBoolean());
        assertFalse(row.has("bid"));
        assertFalse(row.has("ask"));
        assertFalse(row.has("quoteCondition"));
    }

    @Test
    void writesAnEmptySnapshotWithNoGaps() throws Exception {
        dump.onSnapshotBegin(0);
        dump.onSnapshotComplete();
        var lines = bytes.toString(StandardCharsets.UTF_8).lines().toList();
        assertEquals(1, lines.size());
        assertTrue(json.readTree(lines.get(0)).path("gaps").isEmpty());
    }

    @Test
    void preservesBothSidesAndQuoteCondition() throws Exception {
        dump.onSnapshotBegin(10);
        var quote = new BidAsk(Optional.of(new QuoteLevel(new BigDecimal("1E+3"), 0)),
                Optional.of(new QuoteLevel(new BigDecimal("1001.00"), 4_000_000_000L)), OptionalInt.of(7));
        dump.writeQuote("record", 10, 20, Optional.of(quote));
        var row = json.readTree(bytes.toString(StandardCharsets.UTF_8).lines().toList().get(1));
        assertEquals("1000", row.path("bid").path("price").asText());
        assertEquals("1001.00", row.path("ask").path("price").asText());
        assertEquals(4_000_000_000L, row.path("ask").path("size").asLong());
        assertEquals(7, row.path("quoteCondition").asInt());
    }

    @Test
    void failsWhenStdoutCannotBeWritten() {
        var broken = new PrintStream(new java.io.OutputStream() {
            @Override public void write(int value) throws IOException {
                throw new IOException("closed pipe");
            }
        });
        var failed = new LusSnapshot(broken);
        failed.onSnapshotBegin(0);
        assertThrows(IOException.class, failed::onSnapshotComplete);
        assertThrows(IOException.class, () -> failed.writeQuote("record", 10, 20, Optional.empty()));
    }
}
