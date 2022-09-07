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
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/uri.h"

namespace arrow {

using ::arrow::internal::UriFromAbsolutePath;
using internal::checked_cast;
using internal::make_unique;

namespace engine {

template <typename RelMessage>
bool HasEmit(const RelMessage& rel) {
  if (rel.has_common()) {
    return rel.common().has_emit();
  }
  return false;
}

template <typename RelMessage>
Result<std::vector<compute::Expression>> GetEmitInfo(
    const RelMessage& rel, const std::shared_ptr<Schema>& schema) {
  const auto& emit = rel.common().emit();
  int emit_size = emit.output_mapping_size();
  std::vector<compute::Expression> proj_field_refs(emit_size);
  for (int i = 0; i < emit_size; i++) {
    int32_t map_id = emit.output_mapping(i);
    proj_field_refs[i] = compute::field_ref(FieldRef(map_id));
  }
  return std::move(proj_field_refs);
}

template <typename RelMessage>
Result<DeclarationInfo> ProcessEmit(const RelMessage& rel,
                                    const DeclarationInfo& no_emit_declr,
                                    const std::shared_ptr<Schema>& schema) {
  if (rel.has_common()) {
    switch (rel.common().emit_kind_case()) {
      case substrait::RelCommon::EmitKindCase::kDirect:
        return no_emit_declr;
      case substrait::RelCommon::EmitKindCase::kEmit: {
        ARROW_ASSIGN_OR_RAISE(auto emit_expressions, GetEmitInfo(rel, schema));
        return DeclarationInfo{
            compute::Declaration::Sequence(
                {no_emit_declr.declaration,
                 {"project", compute::ProjectNodeOptions{std::move(emit_expressions)}}}),
            std::move(schema)};
      }
      default:
        return Status::Invalid("Invalid emit case");
    }
  } else {
    return no_emit_declr;
  }
}

template <typename RelMessage>
Status CheckRelCommon(const RelMessage& rel) {
  if (rel.has_common()) {
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

      if (read.has_named_table()) {
        if (!conversion_options.named_table_provider) {
          return Status::Invalid(
              "plan contained a named table but a NamedTableProvider has not been "
              "configured");
        }
        const NamedTableProvider& named_table_provider =
            conversion_options.named_table_provider;
        const substrait::ReadRel::NamedTable& named_table = read.named_table();
        std::vector<std::string> table_names(named_table.names().begin(),
                                             named_table.names().end());
        ARROW_ASSIGN_OR_RAISE(compute::Declaration no_emit_declaration,
                              named_table_provider(table_names));
        return ProcessEmit(std::move(read),
                           DeclarationInfo{std::move(no_emit_declaration), base_schema},
                           std::move(base_schema));
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
        switch (item.path_type_case()) {
          case substrait::ReadRel_LocalFiles_FileOrFiles::kUriPath: {
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
            break;
          }
          case substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile: {
            files.emplace_back(path, fs::FileType::File);
            break;
          }
          case substrait::ReadRel_LocalFiles_FileOrFiles::kUriFolder: {
            fs::FileSelector selector;
            selector.base_dir = path;
            selector.recursive = true;
            ARROW_ASSIGN_OR_RAISE(auto discovered_files,
                                  filesystem->GetFileInfo(selector));
            std::move(discovered_files.begin(), discovered_files.end(),
                      std::back_inserter(files));
            break;
          }
          case substrait::ReadRel_LocalFiles_FileOrFiles::kUriPathGlob: {
            ARROW_ASSIGN_OR_RAISE(auto discovered_files,
                                  fs::internal::GlobFiles(filesystem, path));
            std::move(discovered_files.begin(), discovered_files.end(),
                      std::back_inserter(files));
            break;
          }
          default: {
            return Status::Invalid("Unrecognized file type in LocalFiles");
          }
        }
      }

      ARROW_ASSIGN_OR_RAISE(auto ds_factory, dataset::FileSystemDatasetFactory::Make(
                                                 std::move(filesystem), std::move(files),
                                                 std::move(format), {}));

      ARROW_ASSIGN_OR_RAISE(auto ds, ds_factory->Finish(base_schema));

      DeclarationInfo no_emit_declaration = {
          compute::Declaration{"scan", dataset::ScanNodeOptions{ds, scan_options}},
          base_schema};

      return ProcessEmit(std::move(read), std::move(no_emit_declaration),
                         std::move(base_schema));
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
      DeclarationInfo no_emit_declaration{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"filter", compute::FilterNodeOptions{std::move(condition)}},
          }),
          input.output_schema};

      return ProcessEmit(std::move(filter), std::move(no_emit_declaration),
                         input.output_schema);
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
      int num_columns = input.output_schema->num_fields();
      expressions.reserve(num_columns + project.expressions().size());
      for (int i = 0; i < num_columns; i++) {
        expressions.emplace_back(compute::field_ref(FieldRef(i)));
      }

      int i = 0;
      auto project_schema = input.output_schema;
      for (const auto& expr : project.expressions()) {
        std::shared_ptr<Field> project_field;
        ARROW_ASSIGN_OR_RAISE(compute::Expression des_expr,
                              FromProto(expr, ext_set, conversion_options));
        auto bound_expr = des_expr.Bind(*input.output_schema);
        if (auto* expr_call = bound_expr->call()) {
          project_field = field(expr_call->function_name,
                                expr_call->kernel->signature->out_type().type());
        } else if (auto* field_ref = des_expr.field_ref()) {
          ARROW_ASSIGN_OR_RAISE(FieldPath field_path,
                                field_ref->FindOne(*input.output_schema));
          ARROW_ASSIGN_OR_RAISE(project_field, field_path.Get(*input.output_schema));
        } else if (auto* literal = des_expr.literal()) {
          project_field =
              field("field_" + std::to_string(num_columns + i), literal->type());
        }
        ARROW_ASSIGN_OR_RAISE(
            project_schema,
            project_schema->AddField(
                num_columns + static_cast<int>(project.expressions().size()) - 1,
                std::move(project_field)));
        i++;
        expressions.emplace_back(des_expr);
      }

      DeclarationInfo no_emit_declaration{
          compute::Declaration::Sequence({
              std::move(input.declaration),
              {"project", compute::ProjectNodeOptions{std::move(expressions)}},
          }),
          project_schema};

      return ProcessEmit(std::move(project), std::move(no_emit_declaration),
                         std::move(project_schema));
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

      // Create output schema from left, right relations and join keys
      std::shared_ptr<Schema> join_schema = left.output_schema;
      std::shared_ptr<Schema> right_schema = right.output_schema;

      for (const auto& field : right_schema->fields()) {
        ARROW_ASSIGN_OR_RAISE(
            join_schema, join_schema->AddField(
                             static_cast<int>(join_schema->fields().size()) - 1, field));
      }

      compute::HashJoinNodeOptions join_options{{std::move(*left_keys)},
                                                {std::move(*right_keys)}};
      join_options.join_type = join_type;
      join_options.key_cmp = {join_key_cmp};
      compute::Declaration join_dec{"hashjoin", std::move(join_options)};
      join_dec.inputs.emplace_back(std::move(left.declaration));
      join_dec.inputs.emplace_back(std::move(right.declaration));

      DeclarationInfo no_emit_declaration{std::move(join_dec), join_schema};

      return ProcessEmit(std::move(join), std::move(no_emit_declaration),
                         std::move(join_schema));
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

      // prepare output schema from aggregates
      auto input_schema = input.output_schema;
      // store key fields to be used when output schema is created
      std::vector<int> key_field_ids;
      std::vector<FieldRef> keys;
      if (aggregate.groupings_size() > 0) {
        const substrait::AggregateRel::Grouping& group = aggregate.groupings(0);
        int grouping_expr_size = group.grouping_expressions_size();
        keys.reserve(grouping_expr_size);
        key_field_ids.reserve(grouping_expr_size);
        for (int exp_id = 0; exp_id < grouping_expr_size; exp_id++) {
          ARROW_ASSIGN_OR_RAISE(
              compute::Expression expr,
              FromProto(group.grouping_expressions(exp_id), ext_set, conversion_options));
          const FieldRef* field_ref = expr.field_ref();
          if (field_ref) {
            ARROW_ASSIGN_OR_RAISE(auto match, field_ref->FindOne(*input_schema));
            key_field_ids.emplace_back(std::move(match[0]));
            keys.emplace_back(std::move(*field_ref));
          } else {
            return Status::Invalid(
                "The grouping expression for an aggregate must be a direct reference.");
          }
        }
      }

      int measure_size = aggregate.measures_size();
      std::vector<compute::Aggregate> aggregates;
      aggregates.reserve(measure_size);
      // store aggregate fields to be used when output schema is created
      std::vector<int> agg_src_field_ids(measure_size);
      for (int measure_id = 0; measure_id < measure_size; measure_id++) {
        const auto& agg_measure = aggregate.measures(measure_id);
        if (agg_measure.has_measure()) {
          if (agg_measure.has_filter()) {
            return Status::NotImplemented("Aggregate filters are not supported.");
          }
          const auto& agg_func = agg_measure.measure();
          ARROW_ASSIGN_OR_RAISE(
              SubstraitCall aggregate_call,
              FromProto(agg_func, !keys.empty(), ext_set, conversion_options));
          ARROW_ASSIGN_OR_RAISE(
              ExtensionIdRegistry::SubstraitAggregateToArrow converter,
              ext_set.registry()->GetSubstraitAggregateToArrow(aggregate_call.id()));
          ARROW_ASSIGN_OR_RAISE(compute::Aggregate arrow_agg, converter(aggregate_call));

          // find aggregate field ids from schema
          const auto field_ref = arrow_agg.target;
          ARROW_ASSIGN_OR_RAISE(auto match, field_ref.FindOne(*input_schema));
          agg_src_field_ids[measure_id] = match[0];

          aggregates.push_back(std::move(arrow_agg));
        } else {
          return Status::Invalid("substrait::AggregateFunction not provided");
        }
      }
      FieldVector output_fields;
      output_fields.reserve(key_field_ids.size() + agg_src_field_ids.size());
      // extract aggregate fields to output schema
      for (int id = 0; id < static_cast<int>(agg_src_field_ids.size()); id++) {
        output_fields.emplace_back(input_schema->field(agg_src_field_ids[id]));
      }
      // extract key fields to output schema
      for (int id = 0; id < static_cast<int>(key_field_ids.size()); id++) {
        output_fields.emplace_back(input_schema->field(key_field_ids[id]));
      }

      std::shared_ptr<Schema> aggregate_schema = schema(std::move(output_fields));

      DeclarationInfo no_emit_declaration{
          compute::Declaration::Sequence(
              {std::move(input.declaration),
               {"aggregate", compute::AggregateNodeOptions{aggregates, keys}}}),
          aggregate_schema};

      return ProcessEmit(std::move(aggregate), std::move(no_emit_declaration),
                         std::move(aggregate_schema));
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

namespace {

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

Result<std::unique_ptr<substrait::ReadRel>> ScanRelationConverter(
    const std::shared_ptr<Schema>& schema, const compute::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
  auto read_rel = make_unique<substrait::ReadRel>();
  const auto& scan_node_options =
      checked_cast<const dataset::ScanNodeOptions&>(*declaration.options);
  auto dataset =
      dynamic_cast<dataset::FileSystemDataset*>(scan_node_options.dataset.get());
  if (dataset == nullptr) {
    return Status::Invalid(
        "Can only convert scan node with FileSystemDataset to a Substrait plan.");
  }
  // set schema
  ARROW_ASSIGN_OR_RAISE(auto named_struct,
                        ToProto(*dataset->schema(), ext_set, conversion_options));
  read_rel->set_allocated_base_schema(named_struct.release());

  // set local files
  auto read_rel_lfs = make_unique<substrait::ReadRel_LocalFiles>();
  for (const auto& file : dataset->files()) {
    auto read_rel_lfs_ffs = make_unique<substrait::ReadRel_LocalFiles_FileOrFiles>();
    read_rel_lfs_ffs->set_uri_path(UriFromAbsolutePath(file));
    // set file format
    auto format_type_name = dataset->format()->type_name();
    if (format_type_name == "parquet") {
      read_rel_lfs_ffs->set_allocated_parquet(
          new substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions());
    } else if (format_type_name == "ipc") {
      read_rel_lfs_ffs->set_allocated_arrow(
          new substrait::ReadRel::LocalFiles::FileOrFiles::ArrowReadOptions());
    } else if (format_type_name == "orc") {
      read_rel_lfs_ffs->set_allocated_orc(
          new substrait::ReadRel::LocalFiles::FileOrFiles::OrcReadOptions());
    } else {
      return Status::NotImplemented("Unsupported file type: ", format_type_name);
    }
    read_rel_lfs->mutable_items()->AddAllocated(read_rel_lfs_ffs.release());
  }
  read_rel->set_allocated_local_files(read_rel_lfs.release());
  return std::move(read_rel);
}

Result<std::unique_ptr<substrait::FilterRel>> FilterRelationConverter(
    const std::shared_ptr<Schema>& schema, const compute::Declaration& declaration,
    ExtensionSet* ext_set, const ConversionOptions& conversion_options) {
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

  // handling input
  auto declr_input = declaration.inputs[0];
  ARROW_ASSIGN_OR_RAISE(
      auto input_rel,
      ToProto(util::get<compute::Declaration>(declr_input), ext_set, conversion_options));
  filter_rel->set_allocated_input(input_rel.release());

  ARROW_ASSIGN_OR_RAISE(auto subs_expr,
                        ToProto(bound_expression, ext_set, conversion_options));
  filter_rel->set_allocated_condition(subs_expr.release());
  return std::move(filter_rel);
}

}  // namespace

Status SerializeAndCombineRelations(const compute::Declaration& declaration,
                                    ExtensionSet* ext_set,
                                    std::unique_ptr<substrait::Rel>* rel,
                                    const ConversionOptions& conversion_options) {
  const auto& factory_name = declaration.factory_name;
  ARROW_ASSIGN_OR_RAISE(auto schema, ExtractSchemaToBind(declaration));
  // Note that the sink declaration factory doesn't exist for serialization as
  // Substrait doesn't deal with a sink node definition

  if (factory_name == "scan") {
    ARROW_ASSIGN_OR_RAISE(
        auto read_rel,
        ScanRelationConverter(schema, declaration, ext_set, conversion_options));
    (*rel)->set_allocated_read(read_rel.release());
  } else if (factory_name == "filter") {
    ARROW_ASSIGN_OR_RAISE(
        auto filter_rel,
        FilterRelationConverter(schema, declaration, ext_set, conversion_options));
    (*rel)->set_allocated_filter(filter_rel.release());
  } else if (factory_name == "sink") {
    // Generally when a plan is deserialized the declaration will be a sink declaration.
    // Since there is no Sink relation in substrait, this function would be recursively
    // called on the input of the Sink declaration.
    auto sink_input_decl = util::get<compute::Declaration>(declaration.inputs[0]);
    RETURN_NOT_OK(
        SerializeAndCombineRelations(sink_input_decl, ext_set, rel, conversion_options));
  } else {
    return Status::NotImplemented("Factory ", factory_name,
                                  " not implemented for roundtripping.");
  }

  return Status::OK();
}

Result<std::unique_ptr<substrait::Rel>> ToProto(
    const compute::Declaration& declr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto rel = make_unique<substrait::Rel>();
  RETURN_NOT_OK(SerializeAndCombineRelations(declr, ext_set, &rel, conversion_options));
  return std::move(rel);
}

}  // namespace engine
}  // namespace arrow
