
include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_optimizer .)
add_headers_and_sources(clickhouse_optimizer ./Orca)
add_headers_and_sources(clickhouse_optimizer ./Stats)
add_library(clickhouse_optimizer ${clickhouse_optimizer_headers} ${clickhouse_optimizer_sources})
target_link_libraries(clickhouse_optimizer PUBLIC clickhouse_common_io clickhouse_common_access string_utils _gpos _gpdbcost _gpopt _naucrates ch_contrib::gpxerces)

