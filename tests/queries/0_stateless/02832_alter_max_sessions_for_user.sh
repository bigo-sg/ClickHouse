#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SESSION_ID_PREFIX="02832_alter_max_sessions_session_$$"
QUERY_ID_PREFIX="02832_alter_max_sessions_query_$$"
PROFILE="02832_alter_max_sessions_profile_$$"
USER="02832_alter_max_sessions_user_$$"
USER2="02832_alter_max_sessions_user_two_$$"
ROLE="02832_alter_max_sessions_role_$$"

${CLICKHOUSE_CLIENT} -q $"DROP USER IF EXISTS '${USER}'"
${CLICKHOUSE_CLIENT} -q $"DROP PROFILE IF EXISTS ${PROFILE}"
${CLICKHOUSE_CLIENT} -q $"CREATE SETTINGS PROFILE ${PROFILE}"
${CLICKHOUSE_CLIENT} -q $"CREATE USER '${USER}' SETTINGS PROFILE '${PROFILE}'"

function run_sessions_set()
{
    local sessions_count="$1"
    local session_check="$2"
    for ((i = 1 ; i <= ${sessions_count} ; i++)); do
        local session_id="${SESSION_ID_PREFIX}_${i}"
        local query_id="${QUERY_ID_PREFIX}_${i}"
        # Write only expected error text
        # More than alter_sessions_count queries will not start.
        ${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&user=${USER}&query_id=${query_id}&session_id=${session_id}&session_check=${session_check}&session_timeout=600&function_sleep_max_microseconds_per_block=120000000" --data-binary "SELECT sleep(120)" | grep -o  -m 1 'USER_SESSION_LIMIT_EXCEEDED' &
    done

    for ((i = 1 ; i <= ${sessions_count} ; i++)); do
        local query_id="${QUERY_ID_PREFIX}_${i}"
        $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id='$query_id' SYNC" >/dev/null
    done

    wait
}

function test_alter_profile()
{
    local max_session_count="$1"
    local alter_sessions_count="$2"
    echo $"test_alter_profile case: max_session_count ${max_session_count} alter_sessions_count ${alter_sessions_count}"

    ${CLICKHOUSE_CLIENT} -q $"ALTER SETTINGS PROFILE ${PROFILE} SETTINGS max_sessions_for_user = ${max_session_count}"

    # Create sessions with $max_session_count restriction
    run_sessions_set $max_session_count 0

    # Update restriction to $alter_sessions_count
    ${CLICKHOUSE_CLIENT} -q $"ALTER SETTINGS PROFILE ${PROFILE} SETTINGS max_sessions_for_user = ${alter_sessions_count}"

    # Simultaneous sessions should use max settings from profile ($alter_sessions_count)
    run_sessions_set $max_session_count 1
}

test_alter_profile 1 1
test_alter_profile 2 1
test_alter_profile 1 2
test_alter_profile 2 2

${CLICKHOUSE_CLIENT} -q "SELECT 1 SETTINGS max_sessions_for_user = 1" 2>&1 | grep -m 1 -o 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"SET max_sessions_for_user = 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} --max_sessions_for_user=1 -q $"SELECT 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1
# max_sessions_for_user is profile setting
${CLICKHOUSE_CLIENT} -q $"CREATE USER ${USER2} SETTINGS max_sessions_for_user = 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"ALTER USER ${USER} SETTINGS max_sessions_for_user = 1" 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"CREATE ROLE ${ROLE} SETTINGS max_sessions_for_user = 1" 2>&1 | grep -o  -m 1 'READONLY' | head -1
${CLICKHOUSE_CLIENT} -q $"CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q $"ALTER ROLE ${ROLE} SETTINGS max_sessions_for_user = 1 " 2>&1 | grep -o  -m 1 'READONLY' | head -1

${CLICKHOUSE_CLIENT} -q $"DROP USER IF EXISTS '${USER}'"
${CLICKHOUSE_CLIENT} -q $"DROP USER IF EXISTS '${USER2}'"
${CLICKHOUSE_CLIENT} -q $"DROP PROFILE IF EXISTS ${PROFILE}"
${CLICKHOUSE_CLIENT} -q $"DROP ROLE IF EXISTS ${ROLE}"
