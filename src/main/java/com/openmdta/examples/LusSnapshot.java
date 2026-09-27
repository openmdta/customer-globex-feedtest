package com.openmdta.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openmdta.sdk.p_globex.Blocks;
import com.openmdta.sdk.p_globex.Client;
import com.openmdta.sdk.p_globex.Environment;
import com.openmdta.sdk.p_globex.MarketDataUpdate;
import com.openmdta.sdk.p_globex.MdToken;
import com.openmdta.sdk.p_globex.StreamListener;
import com.openmdta.sdk.p_globex.model.stream_0.BidAsk;
import com.openmdta.sdk.p_globex.model.stream_0.QuoteLevel;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Finite, source-wide delayed LUS snapshot; one JSON object per stdout line. */
public final class LusSnapshot implements StreamListener {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final PrintStream output;
    private ObjectNode snapshotHeader;
    private long rows;

    LusSnapshot(PrintStream output) {
        this.output = output;
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: java -jar target/customer-globex-feedtest.jar [credentials.json]");
        }
        String clientId = System.getenv("OPENMDTA_DATA_CLIENT_ID");
        String secret = System.getenv("OPENMDTA_DATA_CLIENT_SECRET");
        if (args.length == 1) {
            var credentials = JSON.readTree(Path.of(args[0]).toFile());
            clientId = credentials.path("id").asText();
            secret = credentials.path("secret").asText();
        }
        if (clientId == null || clientId.isBlank() || secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("Provide a credentials JSON file with id/secret, or OPENMDTA_DATA_CLIENT_ID and OPENMDTA_DATA_CLIENT_SECRET");
        }
        Instant now = Instant.now();
        byte[] token = MdToken.issue(clientId, Base64.getUrlDecoder().decode(secret), Environment.AUDIENCE,
                now, now.plusSeconds(60), List.of(new MdToken.Grant("LUS:ALLE_WERTPAPIERE", "DL")), List.of(), null);
        var dump = new LusSnapshot(System.out);
        System.err.println("Requesting LUS@lus feed snapshot (DL) from " + Environment.WEBSOCKET);
        try (var client = Client.connect(token).get(20, TimeUnit.SECONDS);
             var request = client.datasetLus().quality("DL").streamSnapshot(List.of(Blocks.BID_ASK), dump)) {
            request.completion().get(Long.getLong("snapshot.timeout.seconds", 300L), TimeUnit.SECONDS);
            System.err.println("Snapshot complete: " + dump.rows + " bid/ask records");
        }
    }

    @Override
    public void onSnapshotBegin(long throughMessageId) {
        snapshotHeader = JSON.createObjectNode().put("type", "snapshot").put("dataset", "LUS@lus")
                .put("quality", "DL").put("throughMessageId", Long.toUnsignedString(throughMessageId));
        snapshotHeader.putArray("gaps");
    }

    @Override
    public void onSnapshotGap(long afterMessageId, long throughMessageId) {
        snapshotHeader.withArray("gaps").addObject()
                .put("afterMessageId", Long.toUnsignedString(afterMessageId))
                .put("throughMessageId", Long.toUnsignedString(throughMessageId));
    }

    @Override
    public void onUpdate(MarketDataUpdate update) throws IOException {
        if (update.isBidAskChanged()) {
            writeQuote(update.recordKey(), update.messageId(), update.eventTimeMicros(Blocks.BID_ASK), update.bidAsk());
        }
    }

    @Override
    public void onSnapshotComplete() throws IOException {
        flushSnapshotHeader();
    }

    void writeQuote(String recordKey, long messageId, long eventTimeMicros, Optional<BidAsk> quote) throws IOException {
        flushSnapshotHeader();
        ObjectNode row = JSON.createObjectNode().put("type", "bidAsk").put("dataset", "LUS@lus")
                .put("recordKey", recordKey).put("messageId", Long.toUnsignedString(messageId))
                .put("eventTimeMicros", Long.toUnsignedString(eventTimeMicros)).put("clear", quote.isEmpty());
        quote.ifPresent(value -> {
            writeSide(row, "bid", value.bid());
            writeSide(row, "ask", value.ask());
            row.putNull("quoteCondition");
            value.quoteCondition().ifPresent(condition -> row.put("quoteCondition", condition));
        });
        write(row);
        rows++;
    }

    private void flushSnapshotHeader() throws IOException {
        // Gap callbacks precede all records. Keep them together in the first JSON line.
        if (snapshotHeader != null) {
            write(snapshotHeader);
            snapshotHeader = null;
        }
    }

    private void write(ObjectNode row) throws IOException {
        output.println(row);
        if (output.checkError()) {
            throw new IOException("Could not write snapshot to stdout");
        }
    }

    private static void writeSide(ObjectNode row, String side, Optional<QuoteLevel> level) {
        row.putNull(side);
        level.ifPresent(value -> row.putObject(side).put("price", value.price().toPlainString()).put("size", value.size()));
    }
}
