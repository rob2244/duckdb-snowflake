
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/keyword_helper.hpp"
namespace duckdb {
struct AttachFunctionData : public TableFunctionData {
  bool finished = false;
  string source_schema = "public";
  string sink_schema = "main";
  bool overwrite = false;

  string dsn = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context,
                                           TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types,
                                           vector<string> &names) {
  auto result = make_uniq<AttachFunctionData>();
  result->dsn = input.inputs[0].GetValue<string>();

  for (auto &kv : input.named_parameters) {
    if (kv.first == "source_schema") {
      result->source_schema = StringValue::Get(kv.second);
    } else if (kv.first == "sink_schema") {
      result->sink_schema = StringValue::Get(kv.second);
    } else if (kv.first == "overwrite") {
      result->overwrite = BooleanValue::Get(kv.second);
    }
  }

  return_types.push_back(LogicalType::BOOLEAN);
  names.emplace_back("Success");
  return std::move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p,
                           DataChunk &output) {
  auto &data = (AttachFunctionData &)*data_p.bind_data;
  if (data.finished) {
    return;
  }

  // TODO: get snowflake conn
  // auto conn = SnowflakeConnection::Open(data.dsn);
  auto dconn = Connction(context.db->GetDatabase(context));
  // TODO: decide if we need to filter for only tables
  auto fetch_table_query = StringUtil::Format(
      R"(
SELECT DISTINCT(t.table_name)
FROM information_schema.tables t
WHERE TABLE_SCHEMA = %s
ORDER BY t.table_name;
)",
      KeywordHelper::WriteQuoted(data.source_schema));
}
} // namespace duckdb
