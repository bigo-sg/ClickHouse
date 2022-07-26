# orca optimizer

A CBO-style optimizer based on ORCA， mainly contains the following

#### Optimizer configure builder

It defines the cost model and cluster information

#### Metadata builder

It provides statistics for tables and its columns. And some information about ClickHouse operators and data types.

#### Query to DXL transformer

It transforms a ast query into a DXL that the orca optimizer can handle.

#### DXL to plan transformer

It transform a DXL plan that comes from orca optimizers into a plan that Clickhoue can handle.


## 待解决的问题

type, operator, function这些元信息可以视为静态数据。表和列的信息是会动态变化的，在每次查询时需要重新构建这部分数据，并合并到meta data中。