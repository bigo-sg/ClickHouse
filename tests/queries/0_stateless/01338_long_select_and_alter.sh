#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS alter_mt"

$CLICKHOUSE_CLIENT --query "CREATE TABLE alter_mt (key UInt64, value String) ENGINE=MergeTree() ORDER BY key"

$CLICKHOUSE_CLIENT --query "INSERT INTO alter_mt SELECT number, toString(number) FROM numbers(5)"

$CLICKHOUSE_CLIENT --function_sleep_max_microseconds_per_block 10000000 --query "SELECT count(distinct concat(value, '_')) FROM alter_mt WHERE not sleepEachRow(2)" &

# to be sure that select took all required locks
sleep 2

$CLICKHOUSE_CLIENT --query "ALTER TABLE alter_mt MODIFY COLUMN value UInt64"

$CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM alter_mt"

wait

$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE alter_mt"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS alter_mt"
