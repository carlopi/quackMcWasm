#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void QuackScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) { 
			return StringVector::AddString(result, "🐥 🐥 🐥 Quack "+name.GetString()+" 🐥 🐥 🐥");;
        });


unique_ptr<TableRef> QuackzScanReplacement(ClientContext &context, const string &table_name,
                                            ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	if (!StringUtil::EndsWith(lower_name, ".quack")) {
		return nullptr;
	}

		// This to be refactored away
		char *str = (char *)EM_ASM_PTR({
			console.log(UTF8ToString($0));
			var jsString = "https://quaaack.org/" + UTF8ToString($0) + ".csv";
			var lengthBytes = lengthBytesUTF8(jsString) + 1;
			// 'jsString.length' would return the length of the string as UTF-16
			// units, but Emscripten C strings operate as UTF-8.
			var stringOnWasmHeap = _malloc(lengthBytes);
			stringToUTF8(jsString, stringOnWasmHeap, lengthBytes);
			return stringOnWasmHeap;
		}, table_name.c_str());
		std::string address(str);
		free(str);

		string new_table_name = address;

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(new_table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));
	return std::move(table_function);
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
    con.BeginTransaction();

    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    CreateScalarFunctionInfo quack_fun_info(
            ScalarFunction("quack", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun));
    quack_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &quack_fun_info);
    con.Commit();
	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(QuackzScanReplacement);
}

void QuackExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string QuackExtension::Name() {
	return "quack";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quack_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *quack_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
