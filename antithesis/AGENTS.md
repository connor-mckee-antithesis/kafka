This directory contains files relevant to running tests in Antithesis.

Use the `antithesis-setup` skill to scaffold and manage this directory. Use the `antithesis-research` skill to analyze the system and build a property catalog. Use the `antithesis-workload` skill to implement assertions and test commands. Use the `antithesis-launch` skill to build, validate, and submit Antithesis runs — do not run `snouty launch` directly.

**snouty launch**
Use `snouty launch --json --webhook basic_test --config antithesis/config` to start an Antithesis run. Always run `compose build` first to ensure images are up to date.

**snouty validate**
Use this command to quickly validate changes to the Antithesis scaffolding. See `snouty validate --help` for details.

**setup-complete.sh**
Inject this script into a Dockerfile to notify Antithesis that setup is complete. This script should only run once the system under test is ready for testing. Antithesis will not run any test commands until it receives this event.

**config**
This directory contains the `docker-compose.yaml` file used to bring up this system within the Antithesis environment, along with any closely related config files. Snouty will push tagged images, consume this config directory, and launch the run.

**scratchbook**
This directory is the Antithesis scratchbook for the codebase. It contains documents such as system analysis, property catalogs, topology plans, per-property evidence files (in `scratchbook/properties/`), property relationship maps, and other persistent integration notes. Keep it up to date as Antithesis-related decisions change.

**test**
This directory contains test templates. A test template is a directory containing test command executable files. Each test command must have a valid prefix: `parallel_driver_, singleton_driver_, serial_driver_, first_, eventually_, finally_, anytime_`. Prefixes constrain when and how commands are composed in a single timeline. Files or subdirectories prefixed with `helper_` are ignored by Antithesis and can be used for helper scripts kept alongside the commands.

## Kafka harness layout

- `Dockerfile` — multi-stage build from this checkout. Stage `build` runs
  `./gradlew releaseTarGz`; target `broker` is the KRaft combined broker+controller
  image (`kafka-broker:latest`); target `client` is the workload image
  (`kafka-client:latest`). `Dockerfile.dockerignore` trims the build context.
- `images/broker-entrypoint.sh` — renders `server.properties` from env, formats
  storage once (data survives container restarts), then starts the broker.
- `images/client-entrypoint.sh` — runs `ClientSetup` (wait for 3 brokers, create
  topic `acked`, emit `setup_complete` via the Java SDK), then idles.
- `workload/src/main/java/` — workload Java sources. Compiled with `javac` against
  the release `libs/` plus the Antithesis Java SDK (`/opt/workload/lib/`) and packaged as
  `/opt/workload/app/kafka-antithesis-workload.jar`. Add new workload classes here.
- `test/v1/acked-writes/` — test template copied to `/opt/antithesis/test/v1/acked-writes/`
  in the client image. Run commands with `java -ea -cp "$WORKLOAD_CP" <MainClass>`
  (`WORKLOAD_CP` is set in the client image).
- Instrumentation: broker images catalog the Kafka jars in `/opt/kafka/instrumented`
  (third-party jars stay in `/opt/kafka/libs`); the client catalogs only `/opt/workload/app`.
