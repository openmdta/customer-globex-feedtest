# Globex LUS feed snapshot (Java)

A Java 17 / Maven example using the public
[Globex Java SDK](https://github.com/openmdta/sdk-globex-java) to print the entire
delayed `LUS@lus` bid/ask feed snapshot as newline-delimited JSON, then exit.
It calls `client.dataset("lus").quality("DL").streamSnapshot(...)` and waits for
the gateway's `DONE` response. It does not select individual instruments.

## Build

Requires JDK 17+, Maven 3.9+, `curl`, and `tar`:

```sh
./build.sh
```

Or use the pinned toolchain with `mise run build`.

The script downloads the SDK commit pinned in `pom.xml`, builds it with Maven,
and installs it in your local Maven repository with that commit as its version.
It then runs this project's tests and produces the executable
`target/customer-globex-feedtest.jar`, including its dependencies.
The SDK source stays under the ignored `target/` directory.

This bootstrap is needed because the SDK's JitPack build currently fails with
JitPack's default Maven version. It does not require a GitHub token or an SDK
source checkout. After the initial bootstrap, `mvn verify` rebuilds this project.
Project-local Maven settings use Maven Central instead of inheriting a private
mirror from the developer's `~/.m2/settings.xml`.

## Run with Kursalarm's account

The existing development checkout has an ignored `.credentials.local.json`
containing Kursalarm's deployed data-client ID and current saved signing secret.
The older `../.kursalarm-data-client.json` belongs to a different client and is
not used. From this repository:

```sh
java -jar target/customer-globex-feedtest.jar .credentials.local.json
```

The file has `id` and `secret` keys; `secret` is the base64url-encoded signing key.
The program also accepts the same environment variable names as Kursalarm:
`OPENMDTA_DATA_CLIENT_ID` and `OPENMDTA_DATA_CLIENT_SECRET`. With those set, omit
the filename. A supplied credentials file takes precedence over the environment.
Credentials are read at runtime and are not included in this public repository.
On a fresh checkout, supply your own credentials file or set those variables.

The program signs a 60-second delegated token with the `LUS:ALLE_WERTPAPIERE`
grant at `DL` quality, for audience `openmdta-data:globex`. The SDK connects to
its pinned environment's `wss://globex.openmdta.com/api/v1/ws` endpoint.

To capture only the data:

```sh
java -jar target/customer-globex-feedtest.jar .credentials.local.json > snapshot.ndjson
```

The first stdout line is the snapshot header: dataset, quality, the snapshot's
`throughMessageId`, and any declared gaps. Following lines contain a record key,
message ID, event timestamp, clear flag, bid/ask price and size, and quote condition.
Prices are exact decimal strings; message IDs and microsecond timestamps are
unsigned decimal strings. Missing sides or sizes are `null`; a clear record
contains no bid/ask values. Record keys are the feed's native keys, not assumed
to be ISINs. Candles are history data and are not requested in this snapshot.

Progress and the final record count go to stderr. Authentication errors, timeouts,
protocol errors, and stdout write failures produce a nonzero exit code; a partial
output file is not a completed snapshot. The default timeout is five minutes:

```sh
java -Dsnapshot.timeout.seconds=600 -jar target/customer-globex-feedtest.jar .credentials.local.json
```

The executable JAR supplies Agrona's required `java.base/jdk.internal.misc`
access through its manifest. `.mvn/jvm.config` supplies the same access for Maven.
