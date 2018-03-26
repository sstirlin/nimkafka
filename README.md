# nimkafka

**note: only the low-level wrapper described below currently exists, but the high-level client is coming!**

This package provides both a high-level kafka client (inspired by https://github.com/confluentinc/confluent-kafka-python)
and a low-level wrapper around `librdkafka`.

You need to have `librdkafka` installed and accessible in `LD_LIBRARY_PATH`.

## Example (low level)
See the `example_low_level.nim` file in the `src/` folder for a full-fledged example that both produces and consumes.
