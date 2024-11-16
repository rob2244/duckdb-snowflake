#include "duckdb/common/types.hpp"
#include "duckdb/main/secret/secret.hpp"
#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "snowflake_extension.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void SnowflakeScalarFun(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result,
                                       "Snowflake " + name.GetString() + " 🐥");
        ;
      });
}

inline void SnowflakeOpenSSLVersionScalarFun(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  auto &name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result,
                                       "Snowflake " + name.GetString() +
                                           ", my linked OpenSSL version is " +
                                           OPENSSL_VERSION_TEXT);
        ;
      });
}

unique_ptr<BaseSecret> CreateSnowflakeSecretFunction(ClientContext &context,
                                                     CreateSecretInput &input) {
  // apply any overridden settings
  vector<string> prefix_paths;
  auto result = make_uniq<KeyValueSecret>(prefix_paths, "snowflake", "config",
                                          input.name);
  for (const auto &named_param : input.options) {
    auto lower_name = StringUtil::Lower(named_param.first);

    if (lower_name == "account") {
      result->secret_map["account"] = named_param.second.ToString();
    } else if (lower_name == "user") {
      result->secret_map["user"] = named_param.second.ToString();
    } else if (lower_name == "password") {
      result->secret_map["password"] = named_param.second.ToString();
    } else if (lower_name == "database") {
      result->secret_map["database"] = named_param.second.ToString();
    } else if (lower_name == "schema") {
      result->secret_map["schema"] = named_param.second.ToString();
    } else if (lower_name == "role") {
      result->secret_map["role"] = named_param.second.ToString();
    } else if (lower_name == "warehouse") {
      result->secret_map["warehouse"] = named_param.second.ToString();
    } else if (lower_name == "autocommit") {
      result->secret_map["autocommit"] = named_param.second.ToString();
    } else if (lower_name == "timezone") {
      result->secret_map["timezone"] = named_param.second.ToString();
    } else {
      throw InternalException(
          "Unknown named parameter passed to CreatePostgresSecretFunction: " +
          lower_name);
    }
  }

  //! Set redact keys
  result->redact_keys = {"password"};
  return std::move(result);
}

void SetSnowflakeSecretParameters(CreateSecretFunction &function) {
  function.named_parameters["account"] = LogicalType::VARCHAR;
  function.named_parameters["user"] = LogicalType::VARCHAR;
  function.named_parameters["password"] = LogicalType::VARCHAR;
  function.named_parameters["database"] = LogicalType::VARCHAR;
  function.named_parameters["schema"] = LogicalType::VARCHAR;
  function.named_parameters["role"] = LogicalType::VARCHAR;
  function.named_parameters["warehouse"] = LogicalType::VARCHAR;
}

static void LoadInternal(DatabaseInstance &instance) {
  // Register a scalar function
  auto snowflake_scalar_function =
      ScalarFunction("snowflake", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                     SnowflakeScalarFun);
  ExtensionUtil::RegisterFunction(instance, snowflake_scalar_function);

  // Register another scalar function
  auto snowflake_openssl_version_scalar_function =
      ScalarFunction("snowflake_openssl_version", {LogicalType::VARCHAR},
                     LogicalType::VARCHAR, SnowflakeOpenSSLVersionScalarFun);
  ExtensionUtil::RegisterFunction(instance,
                                  snowflake_openssl_version_scalar_function);

  SecretType secret_type;
  secret_type.name = "snowflake";
  secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
  secret_type.default_provider = "config";

  ExtensionUtil::RegisterSecretType(instance, secret_type);

  CreateSecretFunction snowflake_secrets_function = {
      "postgres", "config", CreateSnowflakeSecretFunction};
  SetSnowflakeSecretParameters(snowflake_secrets_function);
  ExtensionUtil::RegisterFunction(instance, snowflake_secrets_function);
}

void SnowflakeExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string SnowflakeExtension::Name() { return "snowflake"; }

std::string SnowflakeExtension::Version() const {
#ifdef EXT_VERSION_SNOWFLAKE
  return EXT_VERSION_SNOWFLAKE;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void snowflake_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::SnowflakeExtension>();
}

DUCKDB_EXTENSION_API const char *snowflake_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
