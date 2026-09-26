package com.openmdta.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openmdta.sdk.p_globex.BatchView;
import com.openmdta.sdk.p_globex.Blocks;
import com.openmdta.sdk.p_globex.Client;
import com.openmdta.sdk.p_globex.Environment;
import com.openmdta.sdk.p_globex.MdToken;
import com.openmdta.sdk.p_globex.Request;
import com.openmdta.sdk.p_globex.Response;
import com.openmdta.sdk.p_globex.sbe.gateway_protocol.FeedSnapshotHeaderDecoder;
import com.openmdta.sdk.p_globex.sbe.gateway_protocol.MarketDataMessageBatchDecoder;
import com.openmdta.sdk.p_globex.sbe.stream_0.BidAskDecoder;
import com.openmdta.sdk.p_globex.sbe.stream_0.DecimalEncodingDecoder;
import com.openmdta.sdk.p_globex.sbe.stream_0.QuoteLevelDecoder;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Finite, source-wide delayed LUS snapshot; one JSON object per stdout line. */
public final class LusSnapshot implements Request.Listener {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final PrintStream output;
    private final BatchView batch = new BatchView();
    private final BidAskDecoder quote = Blocks.BID_ASK.decoder().get();
    private boolean headerSeen;
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
             var request = client.dataset("lus").quality("DL").streamSnapshot(List.of(Blocks.BID_ASK), dump)) {
            request.completion().get(Long.getLong("snapshot.timeout.seconds", 300L), TimeUnit.SECONDS);
            if (!dump.headerSeen) {
                throw new IOException("Snapshot completed without its header");
            }
            System.err.println("Snapshot complete: " + dump.rows + " bid/ask records");
        }
    }

    @Override
    public void onResponse(Response response) throws IOException {
        if (response.templateId() == FeedSnapshotHeaderDecoder.TEMPLATE_ID) {
            if (headerSeen) {
                throw new IOException("Duplicate snapshot header");
            }
            var header = response.decode(new FeedSnapshotHeaderDecoder());
            ObjectNode row = JSON.createObjectNode().put("type", "snapshot").put("quality", "DL")
                    .put("throughMessageId", Long.toUnsignedString(header.throughMessageId()));
            var gaps = row.putArray("gaps");
            for (var gap : header.gaps()) {
                gaps.addObject().put("afterMessageId", Long.toUnsignedString(gap.afterMessageId()))
                        .put("throughMessageId", Long.toUnsignedString(gap.throughMessageId()));
            }
            byte[] datasetBytes = new byte[header.datasetLength()];
            header.getDataset(datasetBytes, 0, datasetBytes.length);
            String dataset = new String(datasetBytes, StandardCharsets.UTF_8);
            if (!"LUS@lus".equals(dataset)) {
                throw new IOException("Unexpected snapshot dataset: " + dataset);
            }
            row.put("dataset", dataset);
            output.println(row);
            headerSeen = true;
        } else if (response.templateId() == MarketDataMessageBatchDecoder.TEMPLATE_ID) {
            if (!headerSeen) {
                throw new IOException("Snapshot data arrived before its header");
            }
            batch.wrap(response);
            if (!batch.snapshot() || !"LUS@lus".equals(batch.datasetBytes()
                    .getStringWithoutLengthUtf8(0, batch.datasetBytes().capacity()))) {
                throw new IOException("Expected LUS snapshot data");
            }
            batch.forEachField((messageId, field) -> {
                ObjectNode row = JSON.createObjectNode().put("type", "bidAsk").put("dataset", "LUS@lus")
                        .put("recordKey", batch.recordKey()).put("messageId", Long.toUnsignedString(messageId))
                        .put("eventTimeMicros", Long.toUnsignedString(field.eventTimeMicros())).put("clear", field.clear());
                if (!field.clear()) {
                    field.decode(Blocks.BID_ASK, quote);
                    writeSide(row, "bid", quote.bid());
                    writeSide(row, "ask", quote.ask());
                    if (quote.quoteCondition() == BidAskDecoder.quoteConditionNullValue()) {
                        row.putNull("quoteCondition");
                    } else {
                        row.put("quoteCondition", quote.quoteCondition());
                    }
                }
                output.println(row);
                rows++;
            });
        } else {
            throw new IOException("Unexpected snapshot response template: " + response.templateId());
        }
        if (output.checkError()) {
            throw new IOException("Could not write snapshot to stdout");
        }
    }

    private static void writeSide(ObjectNode row, String side, QuoteLevelDecoder level) {
        var price = level.price();
        if (price.mantissa() == DecimalEncodingDecoder.mantissaNullValue()) {
            row.putNull(side);
        } else {
            var value = row.putObject(side).put("price",
                    BigDecimal.valueOf(price.mantissa()).scaleByPowerOfTen(price.exponent()).toPlainString());
            if (level.size() == QuoteLevelDecoder.sizeNullValue()) {
                value.putNull("size");
            } else {
                value.put("size", level.size());
            }
        }
    }
}
