// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/engine/substrait/serde.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/type_resolver_util.h>
#include <gtest/gtest.h>

#include "arrow/compute/exec/expression_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/io/compressed.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/key_value_metadata.h"

#include "parquet/arrow/writer.h"

#include "arrow/util/hash_util.h"
#include "arrow/util/hashing.h"

using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;
using testing::UnorderedElementsAre;

namespace arrow {

using internal::checked_cast;
using internal::hash_combine;
namespace engine {

Status WriteParquetData(const std::string& path,
                        const std::shared_ptr<fs::FileSystem> file_system,
                        const std::shared_ptr<Table> input,
                        const int64_t chunk_size = 3) {
  EXPECT_OK_AND_ASSIGN(auto buffer_writer, file_system->OpenOutputStream(path));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*input, arrow::default_memory_pool(),
                                                  buffer_writer, chunk_size));
  return buffer_writer->Close();
}

// Result<std::string> WriteTemporaryData(const std::string& file_name, const
// std::shared_ptr<Table>& table, const std::shared_ptr<fs::LocalFileSystem>& filesystem)
// {
//   ARROW_ASSIGN_OR_RAISE(auto tempdir,
//   arrow::internal::TemporaryDir::Make("substrait_tempdir")); ARROW_ASSIGN_OR_RAISE(auto
//   file_path, tempdir->path().Join(file_name)); std::string file_path_str =
//   file_path.ToString(); EXPECT_EQ(WriteParquetData(file_path_str, filesystem, table),
//   true); return file_path_str;
// }

bool CompareDataset(std::shared_ptr<dataset::Dataset> ds_lhs,
                    std::shared_ptr<dataset::Dataset> ds_rhs) {
  const auto& fsd_lhs = checked_cast<const dataset::FileSystemDataset&>(*ds_lhs);
  const auto& fsd_rhs = checked_cast<const dataset::FileSystemDataset&>(*ds_rhs);
  const auto& files_lhs = fsd_lhs.files();
  const auto& files_rhs = fsd_rhs.files();

  if (files_lhs.size() != files_rhs.size()) {
    return false;
  }
  uint64_t fidx = 0;
  for (const auto& l_file : files_lhs) {
    if (l_file != files_rhs[fidx++]) {
      return false;
    }
  }
  bool cmp_file_format = fsd_lhs.format()->Equals(*fsd_rhs.format());
  bool cmp_file_system = fsd_lhs.filesystem()->Equals(fsd_rhs.filesystem());
  return cmp_file_format && cmp_file_system;
}

bool CompareScanOptions(const dataset::ScanNodeOptions& lhs,
                        const dataset::ScanNodeOptions& rhs) {
  return lhs.require_sequenced_output == rhs.require_sequenced_output &&
         CompareDataset(lhs.dataset, rhs.dataset);
}

class NullSinkNodeConsumer : public compute::SinkNodeConsumer {
 public:
  Status Init(const std::shared_ptr<Schema>&, compute::BackpressureControl*) override {
    return Status::OK();
  }
  Status Consume(compute::ExecBatch exec_batch) override { return Status::OK(); }
  Future<> Finish() override { return Status::OK(); }

 public:
  static std::shared_ptr<NullSinkNodeConsumer> Make() {
    return std::make_shared<NullSinkNodeConsumer>();
  }
};

const auto kNullConsumer = std::make_shared<NullSinkNodeConsumer>();

const std::shared_ptr<Schema> kBoringSchema = schema({
    field("bool", boolean()),
    field("i8", int8()),
    field("i32", int32()),
    field("i32_req", int32(), /*nullable=*/false),
    field("u32", uint32()),
    field("i64", int64()),
    field("f32", float32()),
    field("f32_req", float32(), /*nullable=*/false),
    field("f64", float64()),
    field("date64", date64()),
    field("str", utf8()),
    field("list_i32", list(int32())),
    field("struct", struct_({
                        field("i32", int32()),
                        field("str", utf8()),
                        field("struct_i32_str",
                              struct_({field("i32", int32()), field("str", utf8())})),
                    })),
    field("list_struct", list(struct_({
                             field("i32", int32()),
                             field("str", utf8()),
                             field("struct_i32_str", struct_({field("i32", int32()),
                                                              field("str", utf8())})),
                         }))),
    field("dict_str", dictionary(int32(), utf8())),
    field("dict_i32", dictionary(int32(), int32())),
    field("ts_ns", timestamp(TimeUnit::NANO)),
});

std::shared_ptr<DataType> StripFieldNames(std::shared_ptr<DataType> type) {
  if (type->id() == Type::STRUCT) {
    FieldVector fields(type->num_fields());
    for (int i = 0; i < type->num_fields(); ++i) {
      fields[i] = type->field(i)->WithName("");
    }
    return struct_(std::move(fields));
  }

  if (type->id() == Type::LIST) {
    return list(type->field(0)->WithName(""));
  }

  return type;
}

inline compute::Expression UseBoringRefs(const compute::Expression& expr) {
  if (expr.literal()) return expr;

  if (auto ref = expr.field_ref()) {
    return compute::field_ref(*ref->FindOne(*kBoringSchema));
  }

  auto modified_call = *CallNotNull(expr);
  for (auto& arg : modified_call.arguments) {
    arg = UseBoringRefs(arg);
  }
  return compute::Expression{std::move(modified_call)};
}

TEST(Substrait, SupportedTypes) {
  auto ExpectEq = [](util::string_view json, std::shared_ptr<DataType> expected_type) {
    ARROW_SCOPED_TRACE(json);

    ExtensionSet empty;
    ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Type", json));
    ASSERT_OK_AND_ASSIGN(auto type, DeserializeType(*buf, empty));

    EXPECT_EQ(*type, *expected_type);

    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeType(*type, &empty));
    EXPECT_EQ(empty.num_types(), 0);

    // FIXME chokes on NULLABILITY_UNSPECIFIED
    // EXPECT_THAT(internal::CheckMessagesEquivalent("Type", *buf, *serialized), Ok());

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeType(*serialized, empty));

    EXPECT_EQ(*roundtripped, *expected_type);
  };

  ExpectEq(R"({"bool": {}})", boolean());

  ExpectEq(R"({"i8": {}})", int8());
  ExpectEq(R"({"i16": {}})", int16());
  ExpectEq(R"({"i32": {}})", int32());
  ExpectEq(R"({"i64": {}})", int64());

  ExpectEq(R"({"fp32": {}})", float32());
  ExpectEq(R"({"fp64": {}})", float64());

  ExpectEq(R"({"string": {}})", utf8());
  ExpectEq(R"({"binary": {}})", binary());

  ExpectEq(R"({"timestamp": {}})", timestamp(TimeUnit::MICRO));
  ExpectEq(R"({"date": {}})", date32());
  ExpectEq(R"({"time": {}})", time64(TimeUnit::MICRO));
  ExpectEq(R"({"timestamp_tz": {}})", timestamp(TimeUnit::MICRO, "UTC"));
  ExpectEq(R"({"interval_year": {}})", interval_year());
  ExpectEq(R"({"interval_day": {}})", interval_day());

  ExpectEq(R"({"uuid": {}})", uuid());

  ExpectEq(R"({"fixed_char": {"length": 32}})", fixed_char(32));
  ExpectEq(R"({"varchar": {"length": 1024}})", varchar(1024));
  ExpectEq(R"({"fixed_binary": {"length": 32}})", fixed_size_binary(32));

  ExpectEq(R"({"decimal": {"precision": 27, "scale": 5}})", decimal128(27, 5));

  ExpectEq(R"({"struct": {
    "types": [
      {"i64": {}},
      {"list": {"type": {"string":{}} }}
    ]
  }})",
           struct_({
               field("", int64()),
               field("", list(utf8())),
           }));

  ExpectEq(R"({"map": {
    "key": {"string":{"nullability": "NULLABILITY_REQUIRED"}},
    "value": {"string":{}}
  }})",
           map(utf8(), field("", utf8()), false));
}

TEST(Substrait, SupportedExtensionTypes) {
  ExtensionSet ext_set;

  for (auto expected_type : {
           null(),
           uint8(),
           uint16(),
           uint32(),
           uint64(),
       }) {
    auto anchor = ext_set.num_types();

    EXPECT_THAT(ext_set.EncodeType(*expected_type), ResultWith(Eq(anchor)));
    ASSERT_OK_AND_ASSIGN(
        auto buf,
        internal::SubstraitFromJSON(
            "Type", "{\"user_defined\": { \"type_reference\": " + std::to_string(anchor) +
                        ", \"nullability\": \"NULLABILITY_NULLABLE\" } }"));

    ASSERT_OK_AND_ASSIGN(auto type, DeserializeType(*buf, ext_set));
    EXPECT_EQ(*type, *expected_type);

    auto size = ext_set.num_types();
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeType(*type, &ext_set));
    EXPECT_EQ(ext_set.num_types(), size) << "was already added to the set above";

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeType(*serialized, ext_set));
    EXPECT_EQ(*roundtripped, *expected_type);
  }
}

TEST(Substrait, NamedStruct) {
  ExtensionSet ext_set;

  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("NamedStruct", R"({
    "struct": {
      "types": [
        {"i64": {}},
        {"list": {"type": {"string":{}} }},
        {"struct": {
          "types": [
            {"fp32": {"nullability": "NULLABILITY_REQUIRED"}},
            {"string": {}}
          ]
        }},
        {"list": {"type": {"string":{}} }},
      ]
    },
    "names": ["a", "b", "c", "d", "e", "f"]
  })"));
  ASSERT_OK_AND_ASSIGN(auto schema, DeserializeSchema(*buf, ext_set));
  Schema expected_schema({
      field("a", int64()),
      field("b", list(utf8())),
      field("c", struct_({
                     field("d", float32(), /*nullable=*/false),
                     field("e", utf8()),
                 })),
      field("f", list(utf8())),
  });
  EXPECT_EQ(*schema, expected_schema);

  ASSERT_OK_AND_ASSIGN(auto serialized, SerializeSchema(*schema, &ext_set));
  ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeSchema(*serialized, ext_set));
  EXPECT_EQ(*roundtripped, expected_schema);

  // too few names
  ASSERT_OK_AND_ASSIGN(buf, internal::SubstraitFromJSON("NamedStruct", R"({
    "struct": {"types": [{"i32": {}}, {"i32": {}}, {"i32": {}}]},
    "names": []
  })"));
  EXPECT_THAT(DeserializeSchema(*buf, ext_set), Raises(StatusCode::Invalid));

  // too many names
  ASSERT_OK_AND_ASSIGN(buf, internal::SubstraitFromJSON("NamedStruct", R"({
    "struct": {"types": []},
    "names": ["a", "b", "c"]
  })"));
  EXPECT_THAT(DeserializeSchema(*buf, ext_set), Raises(StatusCode::Invalid));

  // no schema metadata allowed
  EXPECT_THAT(SerializeSchema(Schema({}, key_value_metadata({{"ext", "yes"}})), &ext_set),
              Raises(StatusCode::Invalid));

  // no schema metadata allowed
  EXPECT_THAT(
      SerializeSchema(Schema({field("a", int32(), key_value_metadata({{"ext", "yes"}}))}),
                      &ext_set),
      Raises(StatusCode::Invalid));
}

TEST(Substrait, NoEquivalentArrowType) {
  ASSERT_OK_AND_ASSIGN(
      auto buf,
      internal::SubstraitFromJSON("Type", R"({"user_defined": {"type_reference": 99}})"));
  ExtensionSet empty;
  ASSERT_THAT(
      DeserializeType(*buf, empty),
      Raises(StatusCode::Invalid, HasSubstr("did not have a corresponding anchor")));
}

TEST(Substrait, NoEquivalentSubstraitType) {
  for (auto type : {
           date64(),
           timestamp(TimeUnit::SECOND),
           timestamp(TimeUnit::NANO),
           timestamp(TimeUnit::MICRO, "New York"),
           time32(TimeUnit::SECOND),
           time32(TimeUnit::MILLI),
           time64(TimeUnit::NANO),

           decimal256(76, 67),

           sparse_union({field("i8", int8()), field("f32", float32())}),
           dense_union({field("i8", int8()), field("f32", float32())}),
           dictionary(int32(), utf8()),

           fixed_size_list(float16(), 3),

           duration(TimeUnit::MICRO),

           large_utf8(),
           large_binary(),
           large_list(utf8()),
       }) {
    ARROW_SCOPED_TRACE(type->ToString());
    ExtensionSet set;
    EXPECT_THAT(SerializeType(*type, &set), Raises(StatusCode::NotImplemented));
  }
}

TEST(Substrait, SupportedLiterals) {
  auto ExpectEq = [](util::string_view json, Datum expected_value) {
    ARROW_SCOPED_TRACE(json);

    ASSERT_OK_AND_ASSIGN(
        auto buf, internal::SubstraitFromJSON("Expression",
                                              "{\"literal\":" + json.to_string() + "}"));
    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto expr, DeserializeExpression(*buf, ext_set));

    ASSERT_TRUE(expr.literal());
    ASSERT_THAT(*expr.literal(), DataEq(expected_value));

    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
    EXPECT_EQ(ext_set.num_functions(), 0);  // shouldn't need extensions for core literals

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));

    ASSERT_TRUE(roundtripped.literal());
    ASSERT_THAT(*roundtripped.literal(), DataEq(expected_value));
  };

  ExpectEq(R"({"boolean": true})", Datum(true));

  ExpectEq(R"({"i8": 34})", Datum(int8_t(34)));
  ExpectEq(R"({"i16": 34})", Datum(int16_t(34)));
  ExpectEq(R"({"i32": 34})", Datum(int32_t(34)));
  ExpectEq(R"({"i64": "34"})", Datum(int64_t(34)));

  ExpectEq(R"({"fp32": 3.5})", Datum(3.5F));
  ExpectEq(R"({"fp64": 7.125})", Datum(7.125));

  ExpectEq(R"({"string": "hello world"})", Datum("hello world"));

  ExpectEq(R"({"binary": "enp6"})", BinaryScalar(Buffer::FromString("zzz")));

  ExpectEq(R"({"timestamp": "579"})", TimestampScalar(579, TimeUnit::MICRO));

  ExpectEq(R"({"date": "5"})", Date32Scalar(5));

  ExpectEq(R"({"time": "64"})", Time64Scalar(64, TimeUnit::MICRO));

  ExpectEq(R"({"interval_year_to_month": {"years": 34, "months": 3}})",
           ExtensionScalar(FixedSizeListScalar(ArrayFromJSON(int32(), "[34, 3]")),
                           interval_year()));

  ExpectEq(R"({"interval_day_to_second": {"days": 34, "seconds": 3}})",
           ExtensionScalar(FixedSizeListScalar(ArrayFromJSON(int32(), "[34, 3]")),
                           interval_day()));

  ExpectEq(R"({"fixed_char": "zzz"})",
           ExtensionScalar(
               FixedSizeBinaryScalar(Buffer::FromString("zzz"), fixed_size_binary(3)),
               fixed_char(3)));

  ExpectEq(R"({"var_char": {"value": "zzz", "length": 1024}})",
           ExtensionScalar(StringScalar("zzz"), varchar(1024)));

  ExpectEq(R"({"fixed_binary": "enp6"})",
           FixedSizeBinaryScalar(Buffer::FromString("zzz"), fixed_size_binary(3)));

  ExpectEq(
      R"({"decimal": {"value": "0gKWSQAAAAAAAAAAAAAAAA==", "precision": 27, "scale": 5}})",
      Decimal128Scalar(Decimal128("123456789.0"), decimal128(27, 5)));

  ExpectEq(R"({"timestamp_tz": "579"})", TimestampScalar(579, TimeUnit::MICRO, "UTC"));

  // special case for empty lists
  ExpectEq(R"({"empty_list": {"type": {"i32": {}}}})",
           ScalarFromJSON(list(int32()), "[]"));

  ExpectEq(R"({"struct": {
    "fields": [
      {"i64": "32"},
      {"list": {"values": [
        {"string": "hello"},
        {"string": "world"}
      ]}}
    ]
  }})",
           ScalarFromJSON(struct_({
                              field("", int64()),
                              field("", list(utf8())),
                          }),
                          R"([32, ["hello", "world"]])"));

  // check null scalars:
  for (auto type : {
           boolean(),

           int8(),
           int64(),

           timestamp(TimeUnit::MICRO),
           interval_year(),

           struct_({
               field("", int64()),
               field("", list(utf8())),
           }),
       }) {
    ExtensionSet set;
    ASSERT_OK_AND_ASSIGN(auto buf, SerializeType(*type, &set));
    ASSERT_OK_AND_ASSIGN(auto json, internal::SubstraitToJSON("Type", *buf));
    ExpectEq("{\"null\": " + json + "}", MakeNullScalar(type));
  }
}

TEST(Substrait, CannotDeserializeLiteral) {
  ExtensionSet ext_set;

  // Invalid: missing List.element_type
  ASSERT_OK_AND_ASSIGN(
      auto buf, internal::SubstraitFromJSON("Expression",
                                            R"({"literal": {"list": {"values": []}}})"));
  EXPECT_THAT(DeserializeExpression(*buf, ext_set), Raises(StatusCode::Invalid));

  // Invalid: required null literal
  ASSERT_OK_AND_ASSIGN(
      buf,
      internal::SubstraitFromJSON(
          "Expression",
          R"({"literal": {"null": {"bool": {"nullability": "NULLABILITY_REQUIRED"}}}})"));
  EXPECT_THAT(DeserializeExpression(*buf, ext_set), Raises(StatusCode::Invalid));

  // no equivalent arrow scalar
  // FIXME no way to specify scalars of user_defined_type_reference
}

TEST(Substrait, FieldRefRoundTrip) {
  for (FieldRef ref : {
           // by name
           FieldRef("i32"),
           FieldRef("ts_ns"),
           FieldRef("struct"),

           // by index
           FieldRef(0),
           FieldRef(1),
           FieldRef(kBoringSchema->num_fields() - 1),
           FieldRef(kBoringSchema->GetFieldIndex("struct")),

           // nested
           FieldRef("struct", "i32"),
           FieldRef("struct", "struct_i32_str", "i32"),
           FieldRef(kBoringSchema->GetFieldIndex("struct"), 1),
       }) {
    ARROW_SCOPED_TRACE(ref.ToString());
    ASSERT_OK_AND_ASSIGN(auto expr, compute::field_ref(ref).Bind(*kBoringSchema));

    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
    EXPECT_EQ(ext_set.num_functions(),
              0);  // shouldn't need extensions for core field references
    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));
    ASSERT_TRUE(roundtripped.field_ref());

    ASSERT_OK_AND_ASSIGN(auto expected, ref.FindOne(*kBoringSchema));
    ASSERT_OK_AND_ASSIGN(auto actual, roundtripped.field_ref()->FindOne(*kBoringSchema));
    EXPECT_EQ(actual.indices(), expected.indices());
  }
}

TEST(Substrait, RecursiveFieldRef) {
  FieldRef ref("struct", "str");

  ARROW_SCOPED_TRACE(ref.ToString());
  ASSERT_OK_AND_ASSIGN(auto expr, compute::field_ref(ref).Bind(*kBoringSchema));
  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
  ASSERT_OK_AND_ASSIGN(auto expected, internal::SubstraitFromJSON("Expression", R"({
    "selection": {
      "directReference": {
        "structField": {
          "field": 12,
          "child": {
            "structField": {
              "field": 1
            }
          }
        }
      },
      "rootReference": {}
    }
  })"));
  ASSERT_OK(internal::CheckMessagesEquivalent("Expression", *serialized, *expected));
}

TEST(Substrait, FieldRefsInExpressions) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       compute::call("struct_field",
                                     {compute::call("if_else",
                                                    {
                                                        compute::literal(true),
                                                        compute::field_ref("struct"),
                                                        compute::field_ref("struct"),
                                                    })},
                                     compute::StructFieldOptions({0}))
                           .Bind(*kBoringSchema));

  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));
  ASSERT_OK_AND_ASSIGN(auto expected, internal::SubstraitFromJSON("Expression", R"({
    "selection": {
      "directReference": {
        "structField": {
          "field": 0
        }
      },
      "expression": {
        "if_then": {
          "ifs": [
            {
              "if": {"literal": {"boolean": true}},
              "then": {"selection": {"directReference": {"structField": {"field": 12}}}}
            }
          ],
          "else": {"selection": {"directReference": {"structField": {"field": 12}}}}
        }
      }
    }
  })"));
  ASSERT_OK(internal::CheckMessagesEquivalent("Expression", *serialized, *expected));
}

TEST(Substrait, CallSpecialCaseRoundTrip) {
  for (compute::Expression expr : {
           compute::call("if_else",
                         {
                             compute::literal(true),
                             compute::field_ref({"struct", 1}),
                             compute::field_ref("str"),
                         }),

           compute::call(
               "case_when",
               {
                   compute::call("make_struct",
                                 {compute::literal(false), compute::literal(true)},
                                 compute::MakeStructOptions({"cond1", "cond2"})),
                   compute::field_ref({"struct", "str"}),
                   compute::field_ref({"struct", "struct_i32_str", "str"}),
                   compute::field_ref("str"),
               }),

           compute::call("list_element",
                         {
                             compute::field_ref("list_i32"),
                             compute::literal(3),
                         }),

           compute::call("struct_field",
                         {compute::call("list_element",
                                        {
                                            compute::field_ref("list_struct"),
                                            compute::literal(42),
                                        })},
                         arrow::compute::StructFieldOptions({1})),

           compute::call("struct_field",
                         {compute::call("list_element",
                                        {
                                            compute::field_ref("list_struct"),
                                            compute::literal(42),
                                        })},
                         arrow::compute::StructFieldOptions({2, 0})),

           compute::call("struct_field",
                         {compute::call("if_else",
                                        {
                                            compute::literal(true),
                                            compute::field_ref("struct"),
                                            compute::field_ref("struct"),
                                        })},
                         compute::StructFieldOptions({0})),
       }) {
    ARROW_SCOPED_TRACE(expr.ToString());
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));

    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));

    // These are special cased as core expressions in substrait; shouldn't require any
    // extensions.
    EXPECT_EQ(ext_set.num_functions(), 0);

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));
    ASSERT_OK_AND_ASSIGN(roundtripped, roundtripped.Bind(*kBoringSchema));
    EXPECT_EQ(UseBoringRefs(roundtripped), UseBoringRefs(expr));
  }
}

TEST(Substrait, CallExtensionFunction) {
  for (compute::Expression expr : {
           compute::call("add", {compute::literal(0), compute::literal(1)}),
       }) {
    ARROW_SCOPED_TRACE(expr.ToString());
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*kBoringSchema));

    ExtensionSet ext_set;
    ASSERT_OK_AND_ASSIGN(auto serialized, SerializeExpression(expr, &ext_set));

    // These require an extension, so we should have a single-element ext_set.
    EXPECT_EQ(ext_set.num_functions(), 1);

    ASSERT_OK_AND_ASSIGN(auto roundtripped, DeserializeExpression(*serialized, ext_set));
    ASSERT_OK_AND_ASSIGN(roundtripped, roundtripped.Bind(*kBoringSchema));
    EXPECT_EQ(UseBoringRefs(roundtripped), UseBoringRefs(expr));
  }
}

TEST(Substrait, ReadRel) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Rel", R"({
    "read": {
      "base_schema": {
        "struct": {
          "types": [ {"i64": {}}, {"bool": {}} ]
        },
        "names": ["i", "b"]
      },
      "filter": {
        "selection": {
          "directReference": {
            "structField": {
              "field": 1
            }
          }
        }
      },
      "local_files": {
        "items": [
          {
            "uri_file": "file:///tmp/dat1.parquet",
            "parquet": {}
          },
          {
            "uri_file": "file:///tmp/dat2.parquet",
            "parquet": {}
          }
        ]
      }
    }
  })"));
  ExtensionSet ext_set;
  ASSERT_OK_AND_ASSIGN(auto rel, DeserializeRelation(*buf, ext_set));

  // converting a ReadRel produces a scan Declaration
  ASSERT_EQ(rel.factory_name, "scan");
  const auto& scan_node_options =
      checked_cast<const dataset::ScanNodeOptions&>(*rel.options);

  // filter on the boolean field (#1)
  EXPECT_EQ(scan_node_options.scan_options->filter, compute::field_ref(1));

  // dataset is a FileSystemDataset in parquet format with the specified schema
  ASSERT_EQ(scan_node_options.dataset->type_name(), "filesystem");
  const auto& dataset =
      checked_cast<const dataset::FileSystemDataset&>(*scan_node_options.dataset);
  EXPECT_THAT(dataset.files(),
              UnorderedElementsAre("/tmp/dat1.parquet", "/tmp/dat2.parquet"));
  EXPECT_EQ(dataset.format()->type_name(), "parquet");
  EXPECT_EQ(*dataset.schema(), Schema({field("i", int64()), field("b", boolean())}));
}

TEST(Substrait, ExtensionSetFromPlan) {
  std::string substrait_json = R"({
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [ {"i64": {}}, {"bool": {}} ]
            },
            "names": ["i", "b"]
          },
          "local_files": { "items": [] }
        }
      }}
    ],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": ")" + default_extension_types_uri() +
                               R"("
      },
      {
        "extension_uri_anchor": 18,
        "uri": ")" + kSubstraitArithmeticFunctionsUri +
                               R"("
      }
    ],
    "extensions": [
      {"extension_type": {
        "extension_uri_reference": 7,
        "type_anchor": 42,
        "name": "null"
      }},
      {"extension_function": {
        "extension_uri_reference": 18,
        "function_anchor": 42,
        "name": "add"
      }}
    ]
})";
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_OK_AND_ASSIGN(auto sink_decls,
                         DeserializePlans(
                             *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));

    EXPECT_OK_AND_ASSIGN(auto decoded_null_type, ext_set.DecodeType(42));
    EXPECT_EQ(decoded_null_type.id.uri, kArrowExtTypesUri);
    EXPECT_EQ(decoded_null_type.id.name, "null");
    EXPECT_EQ(*decoded_null_type.type, NullType());

    EXPECT_OK_AND_ASSIGN(Id decoded_add_func_id, ext_set.DecodeFunction(42));
    EXPECT_EQ(decoded_add_func_id.uri, kSubstraitArithmeticFunctionsUri);
    EXPECT_EQ(decoded_add_func_id.name, "add");
  }
}

TEST(Substrait, ExtensionSetFromPlanMissingFunc) {
  std::string substrait_json = R"({
    "relations": [],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": ")" + default_extension_types_uri() +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "does_not_exist"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(Invalid, DeserializePlans(
                               *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  }
}

TEST(Substrait, ExtensionSetFromPlanExhaustedFactory) {
  std::string substrait_json = R"({
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [ {"i64": {}}, {"bool": {}} ]
            },
            "names": ["i", "b"]
          },
          "local_files": { "items": [] }
        }
      }}
    ],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": ")" + default_extension_types_uri() +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "add"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(
        Invalid,
        DeserializePlans(
            *buf, []() -> std::shared_ptr<compute::SinkNodeConsumer> { return nullptr; },
            ext_id_reg, &ext_set));
    ASSERT_RAISES(
        Invalid,
        DeserializePlans(
            *buf, []() -> std::shared_ptr<dataset::WriteNodeOptions> { return nullptr; },
            ext_id_reg, &ext_set));
  }
}

TEST(Substrait, ExtensionSetFromPlanRegisterFunc) {
  std::string substrait_json = R"({
    "relations": [],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": ")" + default_extension_types_uri() +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "new_func"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));

  auto sp_ext_id_reg = MakeExtensionIdRegistry();
  ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
  // invalid before registration
  ExtensionSet ext_set_invalid(ext_id_reg);
  ASSERT_RAISES(Invalid,
                DeserializePlans(
                    *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set_invalid));
  ASSERT_OK(ext_id_reg->AddSubstraitCallToArrow(
      {default_extension_types_uri(), "new_func"}, "multiply"));
  // valid after registration
  ExtensionSet ext_set_valid(ext_id_reg);
  ASSERT_OK_AND_ASSIGN(auto sink_decls, DeserializePlans(
                                            *buf, [] { return kNullConsumer; },
                                            ext_id_reg, &ext_set_valid));
  EXPECT_OK_AND_ASSIGN(Id decoded_add_func_id, ext_set_valid.DecodeFunction(42));
  EXPECT_EQ(decoded_add_func_id.uri, kArrowExtTypesUri);
  EXPECT_EQ(decoded_add_func_id.name, "new_func");
}

Result<std::string> GetSubstraitJSON() {
  ARROW_ASSIGN_OR_RAISE(std::string dir_string,
                        arrow::internal::GetEnvVar("PARQUET_TEST_DATA"));
  auto file_name =
      arrow::internal::PlatformFilename::FromString(dir_string)->Join("binary.parquet");
  auto file_path = file_name->ToString();

  std::string substrait_json = R"({
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [
                         {"binary": {}}
                       ]
            },
            "names": [
                      "foo"
                      ]
          },
          "local_files": {
            "items": [
              {
                "uri_file": "file://FILENAME_PLACEHOLDER",
                "parquet": {}
              }
            ]
          }
        }
      }}
    ]
  })";
  std::string filename_placeholder = "FILENAME_PLACEHOLDER";
  substrait_json.replace(substrait_json.find(filename_placeholder),
                         filename_placeholder.size(), file_path);
  return substrait_json;
}

TEST(Substrait, DeserializeWithConsumerFactory) {
#ifdef _WIN32
  GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
#else
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
  ASSERT_OK_AND_ASSIGN(auto declarations,
                       DeserializePlans(*buf, NullSinkNodeConsumer::Make));
  ASSERT_EQ(declarations.size(), 1);
  compute::Declaration* decl = &declarations[0];
  ASSERT_EQ(decl->factory_name, "consuming_sink");
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());
  ASSERT_OK_AND_ASSIGN(auto sink_node, declarations[0].AddToPlan(plan.get()));
  ASSERT_STREQ(sink_node->kind_name(), "ConsumingSinkNode");
  ASSERT_EQ(sink_node->num_inputs(), 1);
  auto& prev_node = sink_node->inputs()[0];
  ASSERT_STREQ(prev_node->kind_name(), "SourceNode");

  ASSERT_OK(plan->StartProducing());
  ASSERT_FINISHES_OK(plan->finished());
#endif
}

TEST(Substrait, DeserializeSinglePlanWithConsumerFactory) {
#ifdef _WIN32
  GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
#else
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<compute::ExecPlan> plan,
                       DeserializePlan(*buf, NullSinkNodeConsumer::Make()));
  ASSERT_EQ(1, plan->sinks().size());
  compute::ExecNode* sink_node = plan->sinks()[0];
  ASSERT_STREQ(sink_node->kind_name(), "ConsumingSinkNode");
  ASSERT_EQ(sink_node->num_inputs(), 1);
  auto& prev_node = sink_node->inputs()[0];
  ASSERT_STREQ(prev_node->kind_name(), "SourceNode");

  ASSERT_OK(plan->StartProducing());
  ASSERT_FINISHES_OK(plan->finished());
#endif
}

TEST(Substrait, DeserializeWithWriteOptionsFactory) {
#ifdef _WIN32
  GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
#else
  dataset::internal::Initialize();
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  fs::FileInfo testdir = ::arrow::fs::Dir("testdir");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<fs::FileSystem> fs,
                       fs::internal::MockFileSystem::Make(mock_now, {testdir}));
  auto write_options_factory = [&fs] {
    std::shared_ptr<dataset::IpcFileFormat> format =
        std::make_shared<dataset::IpcFileFormat>();
    dataset::FileSystemDatasetWriteOptions options;
    options.file_write_options = format->DefaultWriteOptions();
    options.filesystem = fs;
    options.basename_template = "chunk-{i}.arrow";
    options.base_dir = "testdir";
    options.partitioning =
        std::make_shared<dataset::DirectoryPartitioning>(arrow::schema({}));
    return std::make_shared<dataset::WriteNodeOptions>(options);
  };
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
  ASSERT_OK_AND_ASSIGN(auto declarations, DeserializePlans(*buf, write_options_factory));
  ASSERT_EQ(declarations.size(), 1);
  compute::Declaration* decl = &declarations[0];
  ASSERT_EQ(decl->factory_name, "write");
  ASSERT_EQ(decl->inputs.size(), 1);
  decl = util::get_if<compute::Declaration>(&decl->inputs[0]);
  ASSERT_NE(decl, nullptr);
  ASSERT_EQ(decl->factory_name, "scan");
  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make());
  ASSERT_OK_AND_ASSIGN(auto sink_node, declarations[0].AddToPlan(plan.get()));
  ASSERT_STREQ(sink_node->kind_name(), "ConsumingSinkNode");
  ASSERT_EQ(sink_node->num_inputs(), 1);
  auto& prev_node = sink_node->inputs()[0];
  ASSERT_STREQ(prev_node->kind_name(), "SourceNode");

  ASSERT_OK(plan->StartProducing());
  ASSERT_FINISHES_OK(plan->finished());
#endif
}

static void test_with_registries(
    std::function<void(ExtensionIdRegistry*, compute::FunctionRegistry*)> test) {
  auto default_func_reg = compute::GetFunctionRegistry();
  auto nested_ext_id_reg = MakeExtensionIdRegistry();
  auto nested_func_reg = compute::FunctionRegistry::Make(default_func_reg);
  test(nullptr, default_func_reg);
  test(nullptr, nested_func_reg.get());
  test(nested_ext_id_reg.get(), default_func_reg);
  test(nested_ext_id_reg.get(), nested_func_reg.get());
}

TEST(Substrait, GetRecordBatchReader) {
#ifdef _WIN32
  GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
#else
  ASSERT_OK_AND_ASSIGN(std::string substrait_json, GetSubstraitJSON());
  test_with_registries([&substrait_json](ExtensionIdRegistry* ext_id_reg,
                                         compute::FunctionRegistry* func_registry) {
    ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
    ASSERT_OK_AND_ASSIGN(auto reader, ExecuteSerializedPlan(*buf));
    ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatchReader(reader.get()));
    // Note: assuming the binary.parquet file contains fixed amount of records
    // in case of a test failure, re-evalaute the content in the file
    EXPECT_EQ(table->num_rows(), 12);
  });
#endif
}

TEST(Substrait, InvalidPlan) {
  std::string substrait_json = R"({
    "relations": [
    ]
  })";
  test_with_registries([&substrait_json](ExtensionIdRegistry* ext_id_reg,
                                         compute::FunctionRegistry* func_registry) {
    ASSERT_OK_AND_ASSIGN(auto buf, SerializeJsonPlan(substrait_json));
    ASSERT_RAISES(Invalid, ExecuteSerializedPlan(*buf));
  });
}

TEST(Substrait, JoinPlanBasic) {
  std::string substrait_json = R"({
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y", "A"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat2.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 5
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitComparisonFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_OK_AND_ASSIGN(auto sink_decls,
                         DeserializePlans(
                             *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));

    auto join_decl = sink_decls[0].inputs[0];

    const auto& join_rel = join_decl.get<compute::Declaration>();

    const auto& join_options =
        checked_cast<const compute::HashJoinNodeOptions&>(*join_rel->options);

    EXPECT_EQ(join_rel->factory_name, "hashjoin");
    EXPECT_EQ(join_options.join_type, compute::JoinType::INNER);

    const auto& left_rel = join_rel->inputs[0].get<compute::Declaration>();
    const auto& right_rel = join_rel->inputs[1].get<compute::Declaration>();

    const auto& l_options =
        checked_cast<const dataset::ScanNodeOptions&>(*left_rel->options);
    const auto& r_options =
        checked_cast<const dataset::ScanNodeOptions&>(*right_rel->options);

    AssertSchemaEqual(
        l_options.dataset->schema(),
        schema({field("A", int32()), field("B", int32()), field("C", int32())}));
    AssertSchemaEqual(
        r_options.dataset->schema(),
        schema({field("X", int32()), field("Y", int32()), field("A", int32())}));

    EXPECT_EQ(join_options.key_cmp[0], compute::JoinKeyCmp::EQ);
  }
}

TEST(Substrait, JoinPlanInvalidKeyCmp) {
  std::string substrait_json = R"({
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y", "A"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat2.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 5
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }],
            "output_type": {
              "bool": {}
            }
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }],
  "extension_uris": [
      {
        "extension_uri_anchor": 0,
        "uri": ")" + std::string(kSubstraitArithmeticFunctionsUri) +
                               R"("
      }
    ],
    "extensions": [
      {"extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "add"
      }}
    ]
  })";
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", substrait_json));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(Invalid, DeserializePlans(
                               *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  }
}

TEST(Substrait, JoinPlanInvalidExpression) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "right": {
          "read": {
            "base_schema": {
              "names": ["X", "Y", "A"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat2.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {"literal": {"list": {"values": []}}},
        "type": "JOIN_TYPE_INNER"
      }
    }
  }]
  })"));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(Invalid, DeserializePlans(
                               *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  }
}

TEST(Substrait, JoinPlanInvalidKeys) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
  "relations": [{
    "rel": {
      "join": {
        "left": {
          "read": {
            "base_schema": {
              "names": ["A", "B", "C"],
              "struct": {
                "types": [{
                  "i32": {}
                }, {
                  "i32": {}
                }, {
                  "i32": {}
                }]
              }
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "file:///tmp/dat1.parquet",
                  "parquet": {}
                }
              ]
            }
          }
        },
        "expression": {
          "scalarFunction": {
            "functionReference": 0,
            "arguments": [{
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 0
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }, {
              "value": {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 5
                    }
                  },
                  "rootReference": {
                  }
                }
              }
            }]
          }
        },
        "type": "JOIN_TYPE_INNER"
      }
    }
  }]
  })"));
  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_RAISES(Invalid, DeserializePlans(
                               *buf, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
  }
}

TEST(Substrait, AggregateBasic) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "arguments": [{
                "value": {
                  "selection": {
                    "directReference": {
                      "structField": {
                        "field": 1
                      }
                    }
                  }
                }
            }],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })"));

  auto sp_ext_id_reg = MakeExtensionIdRegistry();
  ASSERT_OK_AND_ASSIGN(auto sink_decls,
                       DeserializePlans(*buf, [] { return kNullConsumer; }));
  auto agg_decl = sink_decls[0].inputs[0];

  const auto& agg_rel = agg_decl.get<compute::Declaration>();

  const auto& agg_options =
      checked_cast<const compute::AggregateNodeOptions&>(*agg_rel->options);

  EXPECT_EQ(agg_rel->factory_name, "aggregate");
  EXPECT_EQ(agg_options.aggregates[0].name, "");
  EXPECT_EQ(agg_options.aggregates[0].function, "hash_sum");
}

TEST(Substrait, AggregateInvalidRel) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [{
      "rel": {
        "aggregate": {
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })"));

  ASSERT_RAISES(Invalid, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateInvalidFunction) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })"));

  ASSERT_RAISES(Invalid, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateInvalidAggFuncArgs) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "args": [],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "sum"
      }
    }],
  })"));

  ASSERT_RAISES(NotImplemented, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateWithFilter) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "args": [],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_ALL",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/apache/arrow/blob/master/format/substrait/extension_types.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }
    }],
  })"));

  ASSERT_RAISES(NotImplemented, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, AggregateBadPhase) {
  ASSERT_OK_AND_ASSIGN(auto buf, internal::SubstraitFromJSON("Plan", R"({
    "relations": [{
      "rel": {
        "aggregate": {
          "input": {
            "read": {
              "base_schema": {
                "names": ["A", "B", "C"],
                "struct": {
                  "types": [{
                    "i32": {}
                  }, {
                    "i32": {}
                  }, {
                    "i32": {}
                  }]
                }
              },
              "local_files": {
                "items": [
                  {
                    "uri_file": "file:///tmp/dat.parquet",
                    "parquet": {}
                  }
                ]
              }
            }
          },
          "groupings": [{
            "groupingExpressions": [{
              "selection": {
                "directReference": {
                  "structField": {
                    "field": 0
                  }
                }
              }
            }]
          }],
          "measures": [{
            "measure": {
              "functionReference": 0,
              "args": [],
              "sorts": [],
              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
              "invocation": "AGGREGATION_INVOCATION_DISTINCT",
              "outputType": {
                "i64": {}
              }
            }
          }]
        }
      }
    }],
    "extensionUris": [{
      "extension_uri_anchor": 0,
      "uri": "https://github.com/apache/arrow/blob/master/format/substrait/extension_types.yaml"
    }],
    "extensions": [{
      "extension_function": {
        "extension_uri_reference": 0,
        "function_anchor": 0,
        "name": "equal"
      }
    }],
  })"));

  ASSERT_RAISES(NotImplemented, DeserializePlans(*buf, [] { return kNullConsumer; }));
}

TEST(Substrait, SerializePlan) {
#ifdef _WIN32
  GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
#else
  compute::ExecContext exec_context;
  ExtensionSet ext_set;
  auto dummy_schema = schema(
      {field("key", int32()), field("shared", int32()), field("distinct", int32())});
  // creating a dummy dataset using a dummy table

  auto table = TableFromJSON(dummy_schema, {R"([
      [1, 1, 10],
      [3, 4, 20]
    ])",
                                            R"([
      [0, 2, 1],
      [1, 3, 2],
      [4, 1, 3],
      [3, 1, 3],
      [1, 2, 5]
    ])",
                                            R"([
      [2, 2, 12],
      [5, 3, 12],
      [1, 3, 12]
    ])"});

  auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  auto filesystem = std::make_shared<fs::LocalFileSystem>();
  const std::string file_name = "serde_test.parquet";

  ASSERT_OK_AND_ASSIGN(auto tempdir,
                       arrow::internal::TemporaryDir::Make("substrait_tempdir"));
  ASSERT_OK_AND_ASSIGN(auto file_path, tempdir->path().Join(file_name));
  std::string file_path_str = file_path.ToString();

  // Note: there is an additional forward slash introduced by the tempdir
  // it must be replaced to properly load into reading files
  // TODO: (Review: Jira needs to be reported to handle this properly)
  std::string toReplace("/T//");
  size_t pos = file_path_str.find(toReplace);
  file_path_str.replace(pos, toReplace.length(), "/T/");

  ARROW_EXPECT_OK(WriteParquetData(file_path_str, filesystem, table));

  std::vector<fs::FileInfo> files;
  const std::vector<std::string> f_paths = {file_path_str};

  for (const auto& f_path : f_paths) {
    ASSERT_OK_AND_ASSIGN(auto f_file, filesystem->GetFileInfo(f_path));
    files.push_back(std::move(f_file));
  }

  ASSERT_OK_AND_ASSIGN(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                            filesystem, std::move(files), format, {}));
  ASSERT_OK_AND_ASSIGN(auto dataset, ds_factory->Finish(dummy_schema));

  auto scan_options = std::make_shared<dataset::ScanOptions>();
  scan_options->projection = compute::project({}, {});
  const std::string filter_col = "shared";
  auto filter = compute::equal(compute::field_ref(filter_col), compute::literal(3));

  arrow::AsyncGenerator<util::optional<compute::ExecBatch> > sink_gen;

  auto scan_node_options = dataset::ScanNodeOptions{dataset, scan_options};
  auto filter_node_options = compute::FilterNodeOptions{filter};
  auto sink_node_options = compute::SinkNodeOptions{&sink_gen};

  auto scan_declaration = compute::Declaration({"scan", scan_node_options, "s"});
  auto filter_declaration = compute::Declaration({"filter", filter_node_options, "f"});
  auto sink_declaration = compute::Declaration({"sink", sink_node_options, "e"});

  auto declarations = compute::Declaration::Sequence(
      {scan_declaration, filter_declaration, sink_declaration});

  ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make(&exec_context));

  ASSERT_OK_AND_ASSIGN(auto serialized_plan,
                       SerializePlan(plan.get(), declarations, &ext_set));

  for (auto sp_ext_id_reg :
       {std::shared_ptr<ExtensionIdRegistry>(), substrait::MakeExtensionIdRegistry()}) {
    ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
    ExtensionSet ext_set(ext_id_reg);
    ASSERT_OK_AND_ASSIGN(
        auto sink_decls,
        DeserializePlans(
            *serialized_plan, [] { return kNullConsumer; }, ext_id_reg, &ext_set));
    // filter declaration
    auto roundtripped_filter = sink_decls[0].inputs[0].get<compute::Declaration>();
    const auto& filter_opts =
        checked_cast<const compute::FilterNodeOptions&>(*(roundtripped_filter->options));
    auto roundtripped_expr = filter_opts.filter_expression;

    if (auto* call = roundtripped_expr.call()) {
      EXPECT_EQ(call->function_name, "equal");
      auto args = call->arguments;
      auto index = args[0].field_ref()->field_path()->indices()[0];
      EXPECT_EQ(dummy_schema->field_names()[index], filter_col);
      EXPECT_EQ(args[1], compute::literal(3));
    }
    // scan declaration
    auto roundtripped_scan = roundtripped_filter->inputs[0].get<compute::Declaration>();
    const auto& dataset_opts =
        checked_cast<const dataset::ScanNodeOptions&>(*(roundtripped_scan->options));
    const auto& roundripped_ds = dataset_opts.dataset;
    EXPECT_TRUE(roundripped_ds->schema()->Equals(*dummy_schema));
    ASSERT_OK_AND_ASSIGN(auto roundtripped_frgs, roundripped_ds->GetFragments());
    ASSERT_OK_AND_ASSIGN(auto expected_frgs, dataset->GetFragments());

    auto roundtrip_frg_vec = IteratorToVector(std::move(roundtripped_frgs));
    auto expected_frg_vec = IteratorToVector(std::move(expected_frgs));
    EXPECT_EQ(expected_frg_vec.size(), roundtrip_frg_vec.size());
    int64_t idx = 0;
    for (auto fragment : expected_frg_vec) {
      const auto* l_frag = checked_cast<const dataset::FileFragment*>(fragment.get());
      const auto* r_frag =
          checked_cast<const dataset::FileFragment*>(roundtrip_frg_vec[idx++].get());
      EXPECT_TRUE(l_frag->Equals(*r_frag));
    }
  }
#endif
}

// TEST(Substrait, SerializeRelation) {
// #ifdef _WIN32
//   GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
// #else
//   ExtensionSet ext_set;
//   auto dummy_schema = schema({field("a", int32()), field("b", int32())});
//   auto table = TableFromJSON(dummy_schema, {R"([
//     [1, 1],
//     [3, 4]
//                         ])",
//                                             R"([
//     [0, 2],
//     [1, 3],
//     [4, 1],
//     [3, 1],
//     [1, 2]
//                         ])",
//                                             R"([
//     [2, 2],
//     [5, 3],
//     [1, 3]
//                         ])"});
//   const std::string path = "/testing.parquet";

//   EXPECT_OK_AND_ASSIGN(auto filesystem,
//                        fs::internal::MockFileSystem::Make(fs::kNoTime, {}));

//   EXPECT_EQ(WriteParquetData(path, filesystem, table), true);
//   // creating a dummy dataset using a dummy table
//   auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

//   std::vector<fs::FileInfo> files;
//   const std::vector<std::string> f_paths = {path};

//   for (const auto& f_path : f_paths) {
//     ASSERT_OK_AND_ASSIGN(auto f_file, filesystem->GetFileInfo(f_path));
//     files.push_back(std::move(f_file));
//   }

//   ASSERT_OK_AND_ASSIGN(auto ds_factory,
//                        dataset::FileSystemDatasetFactory::Make(
//                            filesystem, std::move(files), std::move(format), {}));
//   ASSERT_OK_AND_ASSIGN(auto dataset, ds_factory->Finish(dummy_schema));

//   auto options = std::make_shared<dataset::ScanOptions>();
//   options->projection = compute::project({}, {});
//   auto scan_node_options = dataset::ScanNodeOptions{dataset, options};

//   auto scan_declaration = compute::Declaration({"scan", scan_node_options});

//   ASSERT_OK_AND_ASSIGN(auto serialized_rel,
//                        SerializeRelation(scan_declaration, &ext_set));
//   ASSERT_OK_AND_ASSIGN(auto deserialized_decl,
//                        DeserializeRelation(*serialized_rel, ext_set));

//   auto& mfs = checked_cast<fs::internal::MockFileSystem&>(*filesystem);
//   mfs.AllFiles();

//   EXPECT_EQ(deserialized_decl.factory_name, scan_declaration.factory_name);
//   const auto& lhs =
//       checked_cast<const dataset::ScanNodeOptions&>(*deserialized_decl.options);
//   const auto& rhs =
//       checked_cast<const dataset::ScanNodeOptions&>(*scan_declaration.options);
//   ASSERT_TRUE(CompareScanOptions(lhs, rhs));
// #endif
// }

// TEST(Substrait, SerializeRelationEndToEnd) {
// #ifdef _WIN32
//   GTEST_SKIP() << "ARROW-16392: Substrait File URI not supported for Windows";
// #else
//   ExtensionSet ext_set;
//   compute::ExecContext exec_context;

//   auto dummy_schema = schema({field("a", int32()), field("b", int32())});
//   auto table = TableFromJSON(dummy_schema, {R"([
//     [1, 1],
//     [3, 4]
//                         ])",
//                                             R"([
//     [0, 2],
//     [1, 3],
//     [4, 1],
//     [3, 1],
//     [1, 2]
//                         ])",
//                                             R"([
//     [2, 2],
//     [5, 3],
//     [1, 3]
//                         ])"});
//   const std::string path = "/testing.parquet";

//   EXPECT_OK_AND_ASSIGN(auto filesystem,
//                        fs::internal::MockFileSystem::Make(fs::kNoTime, {}));

//   EXPECT_EQ(WriteParquetData(path, filesystem, table), true);

//   auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

//   std::vector<fs::FileInfo> files;
//   ASSERT_OK_AND_ASSIGN(auto f_file, filesystem->GetFileInfo(path));
//   files.push_back(std::move(f_file));

//   ASSERT_OK_AND_ASSIGN(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
//                                             filesystem, files, format, {}));
//   ASSERT_OK_AND_ASSIGN(auto other_ds_factory, dataset::FileSystemDatasetFactory::Make(
//                                                   std::move(filesystem),
//                                                   std::move(files), std::move(format),
//                                                   {}));

//   ASSERT_OK_AND_ASSIGN(auto dataset, ds_factory->Finish(dummy_schema));
//   ASSERT_OK_AND_ASSIGN(auto other_dataset, other_ds_factory->Finish(dummy_schema));

//   auto options = std::make_shared<dataset::ScanOptions>();
//   options->projection = compute::project({}, {});

//   auto scan_node_options = dataset::ScanNodeOptions{dataset, options};

//   arrow::AsyncGenerator<util::optional<compute::ExecBatch> > sink_gen;

//   auto sink_node_options = compute::SinkNodeOptions{&sink_gen};

//   auto scan_declaration = compute::Declaration({"scan", scan_node_options});
//   auto sink_declaration = compute::Declaration({"sink", sink_node_options});

//   auto declarations =
//       compute::Declaration::Sequence({scan_declaration, sink_declaration});

//   ASSERT_OK_AND_ASSIGN(auto plan, compute::ExecPlan::Make(&exec_context));
//   ASSERT_OK_AND_ASSIGN(auto decl, declarations.AddToPlan(plan.get()));

//   ASSERT_OK(decl->Validate());

//   std::shared_ptr<arrow::RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
//       dummy_schema, std::move(sink_gen), exec_context.memory_pool());

//   ASSERT_OK(plan->Validate());
//   ASSERT_OK(plan->StartProducing());

//   std::shared_ptr<arrow::Table> response_table;

//   ASSERT_OK_AND_ASSIGN(response_table,
//                        arrow::Table::FromRecordBatchReader(sink_reader.get()));

//   auto other_scan_node_options = dataset::ScanNodeOptions{other_dataset, options};
//   auto other_scan_declaration = compute::Declaration({"scan",
//   other_scan_node_options});

//   ASSERT_OK_AND_ASSIGN(auto serialized_rel,
//                        SerializeRelation(other_scan_declaration, &ext_set));
//   ASSERT_OK_AND_ASSIGN(auto deserialized_decl,
//                        DeserializeRelation(*serialized_rel, ext_set));

//   // arrow::AsyncGenerator<util::optional<compute::ExecBatch> > des_sink_gen;
//   // auto des_sink_node_options = compute::SinkNodeOptions{&des_sink_gen};

//   // auto des_sink_declaration = compute::Declaration({"sink", des_sink_node_options});

//   // auto t_decls =
//   //     compute::Declaration::Sequence({deserialized_decl, des_sink_declaration});

//   // ASSERT_OK_AND_ASSIGN(auto t_plan, compute::ExecPlan::Make());
//   // ASSERT_OK_AND_ASSIGN(auto t_decl, t_decls.AddToPlan(t_plan.get()));

//   // ASSERT_OK(t_decl->Validate());

//   // std::shared_ptr<arrow::RecordBatchReader> des_sink_reader =
//   //     compute::MakeGeneratorReader(dummy_schema, std::move(des_sink_gen),
//   //                                  exec_context.memory_pool());

//   // ASSERT_OK(t_plan->Validate());
//   // ASSERT_OK(t_plan->StartProducing());

//   // std::shared_ptr<arrow::Table> des_response_table;

//   // ASSERT_OK_AND_ASSIGN(des_response_table,
//   //                      arrow::Table::FromRecordBatchReader(des_sink_reader.get()));

//   // ASSERT_TRUE(response_table->Equals(*des_response_table, true));
// #endif
// }

}  // namespace engine
}  // namespace arrow
