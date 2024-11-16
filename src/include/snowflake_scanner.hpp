#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class PostgresAttachFunction : public TableFunction {
public:
  PostgresAttachFunction();
};

} // namespace duckdb
