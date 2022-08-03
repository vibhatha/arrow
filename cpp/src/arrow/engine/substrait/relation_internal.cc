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

#include "arrow/engine/substrait/relation_internal.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/options.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/registry.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"

namespace arrow {

using internal::checked_cast;
using internal::make_unique;

namespace engine {

template <typename RelMessage>
Status CheckRelCommon(const RelMessage& rel) {
  if (rel.has_common()) {
    if (rel.common().has_emit()) {
      return Status::NotImplemented("substrait::RelCommon::Emit");
    }
    if (rel.common().has_hint()) {
      return Status::NotImplemented("substrait::RelCommon::Hint");
    }
    if (rel.common().has_advanced_extension()) {
      return Status::NotImplemented("substrait::RelCommon::advanced_extension");
    }
  }
  if (rel.has_advanced_extension()) {
    return Status::NotImplemented("substrait AdvancedExtensions");
  }
  return Status::OK();
}

Result<DeclarationInfo> FromProto(const substrait::Rel& rel, const ExtensionSet& ext_set,
                                  const ConversionOptions& conversion_options) {
  static bool dataset_init = false;
  if (!dataset_init) {
    dataset_init = true;
    dataset::internal::Initialize();
  }

  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead: {
      const auto& read = rel.read();
      RETURN_NOT_OK(CheckRelCommon(read));

      ARROW_ASSIGN_OR_RAISE(auto base_schema,
                            FromProto(read.base_schema(), ext_set, conversion_options));

      auto scan_options = std::make_shared<dataset::ScanOptions>();
      scan_options->use_threads = true;

      if (read.has_filter()) {
        ARROW_ASSIGN_OR_RAISE(scan_options->filter,
                              FromProto(read.filter(), ext_set, conversion_options));
      }

      if (read.has_projection()) {
        // NOTE: scan_options->projection is not used by the scanner and thus can't be
        // used for this
        return Status::NotImplemented("substrait::ReadRel::projection");
      }

      if (!read.has_local_files()) {
        return Status::NotImplemented(
            "substrait::ReadRel with read_type other than LocalFiles");
      }

      if (read.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::ReadRel::LocalFiles::advanced_extension");
      }

      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();
      std::vector<fs::FileInfo> files;

      for (const auto& item : read.local_files().items()) {
        std::string path;
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriPath) {
          path = item.uri_path();
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          path = item.uri_file();
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFolder) {
          path = item.uri_folder();
        } else {
          path = item.uri_path_glob();
        }

        switch (item.file_format_case()) {
          case substrait::ReadRel_LocalFiles_FileOrFiles::kParquet:
            format = std::make_shared<dataset::ParquetFileFormat>();
            break;
          case substrait::ReadRel_LocalFiles_FileOrFiles::kArrow:
            format = std::make_shared<dataset::IpcFileFormat>();
            break;
          default:
            return Status::NotImplemented(
                "unknown substrait::ReadRel::LocalFiles::FileOrFiles::file_format");
        }

        if (!util::string_view{path}.starts_with("file:///")) {
          return Status::NotImplemented("substrait::ReadRel::LocalFiles item (", path,
                                        ") with other than local filesystem "
                                        "(file:///)");
        }

        if (item.partition_index() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::partition_index");
        }

        if (item.start() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::start offset");
        }

        if (item.length() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::length");
        }

        path = path.substr(7);
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriPath) {
          ARROW_ASSIGN_OR_RAISE(auto file, filesystem->GetFileInfo(path));
          if (file.type() == fs::FileType::File) {
            files.push_back(std::move(file));
          } else if (file.type() == fs::FileType::Directory) {
            fs::FileSelector selector;
            selector.base_dir = path;
            selector.recursive = true;
            ARROW_ASSIGN_OR_RAISE(auto discovered_files,
                                  filesystem->GetFileInfo(selector));
            std::move(files.begin(), files.end(), std::back_inserter(discovered_files));
          }
        }
        if (item.path_type_case() ==
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          files.emplace_back(path, fs::FileType::File);
        } else if (item.path_type_case() ==
                   substrait::ReadRel_LocalFiles_FileOrFiles::kUriFolder) {
          fs::FileSelector selector;
          selector.base_dir = path;
          selector.recursive = true;
          ARROW_ASSIGN_OR_RAISE(auto discovered_files, filesystem->GetFileInfo(selector));
          std::move(discovered_files.begin(), discovered_files.end(),
                    std::back_inserter(files));
        } else {
          ARROW_ASSIGN_OR_RAISE(auto discovered_files,
                                fs::internal::GlobFiles(filesystem, path));
          std::move(discovered_files.begin(), discovered_files.end(),
                    std::back_inserter(files));
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                                 std::move(filesystem), std::move(files),
                                                 std::move(format), {}));

      auto num_columns = static_cast<int>(base_schema->fields().size());
      ARROW_ASSIGN_OR_RAISE(auto ds, ds_factory->Finish(std::move(base_schema)));

      return DeclarationInfo{
          compute::Declaration{
              "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}},
          num_columns};
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(filter.input(), ext_set, conversion_options));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition,
                            FromProto(filter.condition(), ext_set, conversion_options));

      return DeclarationInfo{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"filter", compute::FilterNodeOptions{std::move(condition)}},
          }),
          input.num_columns};
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project));

      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(project.input(), ext_set, conversion_options));

      // NOTE: Substrait ProjectRels *append* columns, while Acero's project node replaces
      // them. Therefore, we need to prefix all the current columns for compatibility.
      std::vector<compute::Expression> expressions;
      expressions.reserve(input.num_columns + project.expressions().size());
      for (int i = 0; i < input.num_columns; i++) {
        expressions.emplace_back(compute::field_ref(FieldRef(i)));
      }
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(),
                              FromProto(expr, ext_set, conversion_options));
      }

      auto num_columns = static_cast<int>(expressions.size());
      return DeclarationInfo{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"project", compute::ProjectNodeOptions{std::move(expressions)}},
          }),
          num_columns};
    }

    case substrait::Rel::RelTypeCase::kJoin: {
      const auto& join = rel.join();
      RETURN_NOT_OK(CheckRelCommon(join));

      if (!join.has_left()) {
        return Status::Invalid("substrait::JoinRel with no left relation");
      }

      if (!join.has_right()) {
        return Status::Invalid("substrait::JoinRel with no right relation");
      }

      compute::JoinType join_type;
      switch (join.type()) {
        case substrait::JoinRel::JOIN_TYPE_UNSPECIFIED:
          return Status::NotImplemented("Unspecified join type is not supported");
        case substrait::JoinRel::JOIN_TYPE_INNER:
          join_type = compute::JoinType::INNER;
          break;
        case substrait::JoinRel::JOIN_TYPE_OUTER:
          join_type = compute::JoinType::FULL_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_LEFT:
          join_type = compute::JoinType::LEFT_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_RIGHT:
          join_type = compute::JoinType::RIGHT_OUTER;
          break;
        case substrait::JoinRel::JOIN_TYPE_SEMI:
          join_type = compute::JoinType::LEFT_SEMI;
          break;
        case substrait::JoinRel::JOIN_TYPE_ANTI:
          join_type = compute::JoinType::LEFT_ANTI;
          break;
        default:
          return Status::Invalid("Unsupported join type");
      }

      ARROW_ASSIGN_OR_RAISE(auto left,
                            FromProto(join.left(), ext_set, conversion_options));
      ARROW_ASSIGN_OR_RAISE(auto right,
                            FromProto(join.right(), ext_set, conversion_options));

      if (!join.has_expression()) {
        return Status::Invalid("substrait::JoinRel with no expression");
      }

      ARROW_ASSIGN_OR_RAISE(auto expression,
                            FromProto(join.expression(), ext_set, conversion_options));

      const auto* callptr = expression.call();
      if (!callptr) {
        return Status::Invalid(
            "A join rel's expression must be a simple equality between keys but got ",
            expression.ToString());
      }

      compute::JoinKeyCmp join_key_cmp;
      if (callptr->function_name == "equal") {
        join_key_cmp = compute::JoinKeyCmp::EQ;
      } else if (callptr->function_name == "is_not_distinct_from") {
        join_key_cmp = compute::JoinKeyCmp::IS;
      } else {
        return Status::Invalid(
            "Only `equal` or `is_not_distinct_from` are supported for join key "
            "comparison but got ",
            callptr->function_name);
      }

      // TODO: ARROW-16624 Add Suffix support for Substrait
      const auto* left_keys = callptr->arguments[0].field_ref();
      const auto* right_keys = callptr->arguments[1].field_ref();
      if (!left_keys || !right_keys) {
        return Status::Invalid("Left keys for join cannot be null");
      }
      compute::HashJoinNodeOptions join_options{{std::move(*left_keys)},
                                                {std::move(*right_keys)}};
      join_options.join_type = join_type;
      join_options.key_cmp = {join_key_cmp};
      compute::Declaration join_dec{"hashjoin", std::move(join_options)};
      auto num_columns = left.num_columns + right.num_columns;
      join_dec.inputs.emplace_back(std::move(left.declaration));
      join_dec.inputs.emplace_back(std::move(right.declaration));
      return DeclarationInfo{std::move(join_dec), num_columns};
    }
    case substrait::Rel::RelTypeCase::kAggregate: {
      const auto& aggregate = rel.aggregate();
      RETURN_NOT_OK(CheckRelCommon(aggregate));

      if (!aggregate.has_input()) {
        return Status::Invalid("substrait::AggregateRel with no input relation");
      }

      ARROW_ASSIGN_OR_RAISE(auto input,
                            FromProto(aggregate.input(), ext_set, conversion_options));

      if (aggregate.groupings_size() > 1) {
        return Status::NotImplemented(
            "Grouping sets not supported.  AggregateRel::groupings may not have more "
            "than one item");
      }
      std::vector<FieldRef> keys;
      auto group = aggregate.groupings(0);
      keys.reserve(group.grouping_expressions_size());
      for (int exp_id = 0; exp_id < group.grouping_expressions_size(); exp_id++) {
        ARROW_ASSIGN_OR_RAISE(auto expr, FromProto(group.grouping_expressions(exp_id),
                                                   ext_set, conversion_options));
        const auto* field_ref = expr.field_ref();
        if (field_ref) {
          keys.emplace_back(std::move(*field_ref));
        } else {
          return Status::Invalid(
              "The grouping expression for an aggregate must be a direct reference.");
        }
      }

      int measure_size = aggregate.measures_size();
      std::vector<compute::Aggregate> aggregates;
      aggregates.reserve(measure_size);
      for (int measure_id = 0; measure_id < measure_size; measure_id++) {
        const auto& agg_measure = aggregate.measures(measure_id);
        if (agg_measure.has_measure()) {
          if (agg_measure.has_filter()) {
            return Status::NotImplemented("Aggregate filters are not supported.");
          }
          const auto& agg_func = agg_measure.measure();
          if (agg_func.arguments_size() != 1) {
            return Status::NotImplemented("Aggregate function must be a unary function.");
          }
          int func_reference = agg_func.function_reference();
          ARROW_ASSIGN_OR_RAISE(auto func_record, ext_set.DecodeFunction(func_reference));
          // aggreagte function name
          auto func_name = std::string(func_record.id.name);
          // aggregate target
          auto subs_func_args = agg_func.arguments(0);
          ARROW_ASSIGN_OR_RAISE(auto field_expr, FromProto(subs_func_args.value(),
                                                           ext_set, conversion_options));
          auto target = field_expr.field_ref();
          if (!target) {
            return Status::Invalid(
                "The input expression to an aggregate function must be a direct "
                "reference.");
          }
          aggregates.emplace_back(compute::Aggregate{std::move(func_name), NULLPTR,
                                                     std::move(*target), std::move("")});
        } else {
          return Status::Invalid("substrait::AggregateFunction not provided");
        }
      }

      return DeclarationInfo{
          compute::Declaration::Sequence(
              {std::move(input.declaration),
               {"aggregate", compute::AggregateNodeOptions{aggregates, keys}}}),
          static_cast<int>(aggregates.size())};
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

Result<std::unique_ptr<substrait::Rel>> ToProto(
    const compute::Declaration& declr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto rel = make_unique<substrait::Rel>();
  RETURN_NOT_OK(SerializeAndCombineRelations(declr, ext_set, rel, conversion_options));
  return std::move(rel);
}

Status SetRelation(const std::unique_ptr<substrait::Rel>& plan,
                   const std::unique_ptr<substrait::Rel>& partial_plan,
                   const std::string& factory_name) {
  if (factory_name == "scan" && partial_plan->has_read()) {
    plan->set_allocated_read(partial_plan->release_read());
  } else if (factory_name == "filter" && partial_plan->has_filter()) {
    plan->set_allocated_filter(partial_plan->release_filter());
  } else {
    return Status::NotImplemented("Substrait converter ", factory_name,
                                  " not supported.");
  }
  return Status::OK();
}

Result<std::shared_ptr<Schema>> ExtractSchemaToBind(const compute::Declaration& declr) {
  std::shared_ptr<Schema> bind_schema;
  if (declr.factory_name == "scan") {
    const auto& opts = checked_cast<const dataset::ScanNodeOptions&>(*(declr.options));
    bind_schema = opts.dataset->schema();
  } else if (declr.factory_name == "filter") {
    auto input_declr = util::get<compute::Declaration>(declr.inputs[0]);
    ARROW_ASSIGN_OR_RAISE(bind_schema, ExtractSchemaToBind(input_declr));
  } else if (declr.factory_name == "sink") {
    // Note that the sink has no output_schema
    return bind_schema;
  } else {
    return Status::Invalid("Schema extraction failed, unsupported factory ",
                           declr.factory_name);
  }
  return bind_schema;
}

Status SerializeAndCombineRelations(const compute::Declaration& declaration,
                                    ExtensionSet* ext_set,
                                    std::unique_ptr<substrait::Rel>& rel,
                                    const ConversionOptions& conversion_options) {
  std::vector<compute::Declaration::Input> inputs = declaration.inputs;
  for (auto& input : inputs) {
    auto input_decl = util::get<compute::Declaration>(input);
    RETURN_NOT_OK(
        SerializeAndCombineRelations(input_decl, ext_set, rel, conversion_options));
  }
  const auto& factory_name = declaration.factory_name;
  ARROW_ASSIGN_OR_RAISE(auto schema, ExtractSchemaToBind(declaration));
  SubstraitConversionRegistry* registry = default_substrait_conversion_registry();
  if (factory_name != "sink") {
    ARROW_ASSIGN_OR_RAISE(auto factory, registry->GetConverter(factory_name));
    ARROW_ASSIGN_OR_RAISE(auto factory_rel,
                          factory(schema, declaration, ext_set, conversion_options));
    RETURN_NOT_OK(SetRelation(rel, factory_rel, factory_name));
  }
  return Status::OK();
}

Result<std::unique_ptr<substrait::Rel>> GetRelationFromDeclaration(
    const compute::Declaration& declaration, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto declr_input = declaration.inputs[0];
  // Note that the input is expected in declaration.
  // ExecNode inputs are not accepted
  if (util::get_if<compute::ExecNode*>(&declr_input)) {
    return Status::NotImplemented("Only support Plans written in Declaration format.");
  }
  return ToProto(util::get<compute::Declaration>(declr_input), ext_set,
                 conversion_options);
}

Result<std::unique_ptr<substrait::Rel>> ScanRelationConverter(
    const std::shared_ptr<Schema>& schema, const compute::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
  auto rel = make_unique<substrait::Rel>();
  auto read_rel = make_unique<substrait::ReadRel>();
  const auto& scan_node_options =
      checked_cast<const dataset::ScanNodeOptions&>(*declaration.options);
  auto dataset =
      dynamic_cast<dataset::FileSystemDataset*>(scan_node_options.dataset.get());
  if (dataset == nullptr) {
    return Status::Invalid("Can only convert file system datasets to a Substrait plan.");
  }
  // set schema
  ARROW_ASSIGN_OR_RAISE(auto named_struct,
                        ToProto(*dataset->schema(), ext_set, conversion_options));
  read_rel->set_allocated_base_schema(named_struct.release());

  // set local files
  auto read_rel_lfs = make_unique<substrait::ReadRel_LocalFiles>();
  for (const auto& file : dataset->files()) {
    auto read_rel_lfs_ffs = make_unique<substrait::ReadRel_LocalFiles_FileOrFiles>();
    read_rel_lfs_ffs->set_uri_path("file://" + file);
    // set file format
    // arrow and feather are temporarily handled via the Parquet format until
    // upgraded to the latest Substrait version.
    auto format_type_name = dataset->format()->type_name();
    if (format_type_name == "parquet") {
      auto parquet_fmt =
          make_unique<substrait::ReadRel_LocalFiles_FileOrFiles_ParquetReadOptions>();
      read_rel_lfs_ffs->set_allocated_parquet(parquet_fmt.release());
    } else if (format_type_name == "arrow") {
      auto arrow_fmt =
          make_unique<substrait::ReadRel_LocalFiles_FileOrFiles_ArrowReadOptions>();
      read_rel_lfs_ffs->set_allocated_arrow(arrow_fmt.release());
    } else if (format_type_name == "orc") {
      auto orc_fmt =
          make_unique<substrait::ReadRel_LocalFiles_FileOrFiles_OrcReadOptions>();
      read_rel_lfs_ffs->set_allocated_orc(orc_fmt.release());
    } else {
      return Status::NotImplemented("Unsupported file type: ", format_type_name);
    }
    read_rel_lfs->mutable_items()->AddAllocated(read_rel_lfs_ffs.release());
  }
  read_rel->set_allocated_local_files(read_rel_lfs.release());
  rel->set_allocated_read(read_rel.release());
  return std::move(rel);
}

Result<std::unique_ptr<substrait::Rel>> FilterRelationConverter(
    const std::shared_ptr<Schema>& schema, const compute::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
  auto rel = make_unique<substrait::Rel>();
  auto filter_rel = make_unique<substrait::FilterRel>();
  const auto& filter_node_options =
      checked_cast<const compute::FilterNodeOptions&>(*(declaration.options));

  auto filter_expr = filter_node_options.filter_expression;
  compute::Expression bound_expression;
  if (!filter_expr.IsBound()) {
    ARROW_ASSIGN_OR_RAISE(bound_expression, filter_expr.Bind(*schema));
  }

  if (declaration.inputs.size() == 0) {
    return Status::Invalid("Filter node doesn't have an input.");
  }

  auto input_rel = GetRelationFromDeclaration(declaration, ext_set, conversion_options);

  filter_rel->set_allocated_input(input_rel->release());

  ARROW_ASSIGN_OR_RAISE(auto subs_expr,
                        ToProto(bound_expression, ext_set, conversion_options));
  filter_rel->set_allocated_condition(subs_expr.release());
  rel->set_allocated_filter(filter_rel.release());
  return std::move(rel);
}

}  // namespace engine
}  // namespace arrow
