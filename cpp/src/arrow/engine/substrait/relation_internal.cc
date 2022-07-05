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

namespace arrow {
namespace engine {

namespace internal {
using ::arrow::internal::checked_cast;
using ::arrow::internal::make_unique;
}  // namespace internal

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

Result<compute::Declaration> FromProto(const substrait::Rel& rel,
                                       const ExtensionSet& ext_set) {
  static bool dataset_init = false;
  if (!dataset_init) {
    dataset_init = true;
    dataset::internal::Initialize();
  }

  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead: {
      const auto& read = rel.read();
      RETURN_NOT_OK(CheckRelCommon(read));

      ARROW_ASSIGN_OR_RAISE(auto base_schema, FromProto(read.base_schema(), ext_set));

      auto scan_options = std::make_shared<dataset::ScanOptions>();
      scan_options->use_threads = true;

      if (read.has_filter()) {
        ARROW_ASSIGN_OR_RAISE(scan_options->filter, FromProto(read.filter(), ext_set));
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

        if (item.format() ==
            substrait::ReadRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
          format = std::make_shared<dataset::ParquetFileFormat>();
        } else if (util::string_view{path}.ends_with(".arrow")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else if (util::string_view{path}.ends_with(".feather")) {
          format = std::make_shared<dataset::IpcFileFormat>();
        } else {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::format "
              "other than FILE_FORMAT_PARQUET");
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

      ARROW_ASSIGN_OR_RAISE(auto ds, ds_factory->Finish(std::move(base_schema)));

      return compute::Declaration{
          "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}};
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(filter.input(), ext_set));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition, FromProto(filter.condition(), ext_set));

      return compute::Declaration::Sequence({
          std::move(input),
          {"filter", compute::FilterNodeOptions{std::move(condition)}},
      });
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project));

      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(project.input(), ext_set));

      std::vector<compute::Expression> expressions;
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(), FromProto(expr, ext_set));
      }

      return compute::Declaration::Sequence({
          std::move(input),
          {"project", compute::ProjectNodeOptions{std::move(expressions)}},
      });
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

      ARROW_ASSIGN_OR_RAISE(auto left, FromProto(join.left(), ext_set));
      ARROW_ASSIGN_OR_RAISE(auto right, FromProto(join.right(), ext_set));

      if (!join.has_expression()) {
        return Status::Invalid("substrait::JoinRel with no expression");
      }

      ARROW_ASSIGN_OR_RAISE(auto expression, FromProto(join.expression(), ext_set));

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

      // TODO: ARROW-166241 Add Suffix support for Substrait
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
      join_dec.inputs.emplace_back(std::move(left));
      join_dec.inputs.emplace_back(std::move(right));
      return std::move(join_dec);
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

namespace {
// TODO: add other types
enum ArrowRelationType : uint8_t {
  SCAN,
  FILTER,
  PROJECT,
  JOIN,
  AGGREGATE,
};

const std::map<std::string, ArrowRelationType> enum_map{
    {"scan", ArrowRelationType::SCAN},           {"filter", ArrowRelationType::FILTER},
    {"project", ArrowRelationType::PROJECT},     {"hashjoin", ArrowRelationType::JOIN},
    {"aggregate", ArrowRelationType::AGGREGATE},
};

struct ExtractRelation {
  explicit ExtractRelation(substrait::Rel* rel, ExtensionSet* ext_set)
      : rel_(rel), ext_set_(ext_set) {}

  Status AddRelation(const compute::Declaration& declaration) {
    const std::string& rel_name = declaration.factory_name;
    switch (enum_map.find(rel_name)->second) {
      case ArrowRelationType::SCAN:
        return AddReadRelation(declaration);
      case ArrowRelationType::FILTER:
        return Status::NotImplemented("Filter operator not supported.");
      case ArrowRelationType::PROJECT:
        return Status::NotImplemented("Project operator not supported.");
      case ArrowRelationType::JOIN:
        return AddJoinRelation(declaration);
      case ArrowRelationType::AGGREGATE:
        return Status::NotImplemented("Aggregate operator not supported.");
      default:
        return Status::Invalid("Unsupported exec node factory name :", rel_name);
    }
  }

  Status AddReadRelation(const compute::Declaration& declaration) {
    auto read_rel = internal::make_unique<substrait::ReadRel>();
    const auto& scan_node_options =
        internal::checked_cast<const dataset::ScanNodeOptions&>(*declaration.options);

    auto dataset =
        dynamic_cast<dataset::FileSystemDataset*>(scan_node_options.dataset.get());
    if (dataset == nullptr) {
      return Status::Invalid(
          "Can only convert file system datasets to a Substrait plan.");
    }
    // set schema
    ARROW_ASSIGN_OR_RAISE(auto named_struct, ToProto(*dataset->schema(), ext_set_));
    read_rel->set_allocated_base_schema(named_struct.release());

    // set local files
    auto read_rel_lfs = internal::make_unique<substrait::ReadRel_LocalFiles>();
    for (const auto& file : dataset->files()) {
      auto read_rel_lfs_ffs =
          internal::make_unique<substrait::ReadRel_LocalFiles_FileOrFiles>();
      read_rel_lfs_ffs->set_uri_path("file://" + file);

      // set file format
      // arrow and feather are temporarily handled via the Parquet format until
      // upgraded to the latest Substrait version.
      auto format_type_name = dataset->format()->type_name();
      if (format_type_name == "parquet" || format_type_name == "arrow" ||
          format_type_name == "feather") {
        read_rel_lfs_ffs->set_format(
            substrait::ReadRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET);
      } else {
        return Status::Invalid("Unsupported file type : ", format_type_name);
      }
      read_rel_lfs->mutable_items()->AddAllocated(read_rel_lfs_ffs.release());
    }
    *read_rel->mutable_local_files() = *read_rel_lfs.get();

    rel_->set_allocated_read(read_rel.release());
    return Status::OK();
  }

  Status AddJoinRelation(const compute::Declaration& declaration) {
    std::cout << "Add Join Relation" << std::endl;
    auto join_rel = internal::make_unique<substrait::JoinRel>();
    const auto& join_node_options =
        internal::checked_cast<const compute::HashJoinNodeOptions&>(*declaration.options);
    auto get_schemas =
        [](const compute::Declaration& declr) -> std::vector<std::shared_ptr<Schema>> {
      return {};
    };
    auto schemas = get_schemas(declaration);
    ARROW_ASSIGN_OR_RAISE(auto inputs,
                          GetJoinRelationFromDeclaration(declaration, ext_set_));
    if (inputs.size() != 2) {
      return Status::Invalid("Join declaration doesn't include left and right inputs.");
    }
    *join_rel->mutable_left() = std::move(*inputs[0]);
    *join_rel->mutable_right() = std::move(*inputs[1]);
    // unclear point where multiple-column joins are supported in Substrait
    compute::Expression left_key = compute::field_ref(join_node_options.left_keys[0]);
    compute::Expression right_key = compute::field_ref(join_node_options.right_keys[0]);

    // TODO get the schema of the join keys
    // auto left_schema = schema({});
    // auto right_schema = schema({});

    const auto join_key_cmp = join_node_options.key_cmp[0];
    compute::Expression join_expression;
    if (join_key_cmp == compute::JoinKeyCmp::EQ) {
      compute::Expression left_expr = left_key;
      compute::Expression right_expr = right_key;

      // TODO add Bind
      // ARROW_ASSIGN_OR_RAISE(left_expr, left_expr.Bind(*left_schema)));
      // ARROW_ASSIGN_OR_RAISE(right_expr, right_expr.Bind(*right_schema));
      join_expression = compute::equal(left_expr, right_expr);
    } else if (join_key_cmp == compute::JoinKeyCmp::IS) {
      return Status::NotImplemented("is_not_distinct_from not implemented");
    } else {
      return Status::Invalid("Unsupported JoinKeyCmp");
    }
    ARROW_ASSIGN_OR_RAISE(auto sub_join_expr, ToProto(join_expression, ext_set_));
    join_rel->set_allocated_expression(sub_join_expr.release());

    rel_->set_allocated_join(join_rel.release());
    return Status::OK();
  }

  Status operator()(const compute::Declaration& declaration) {
    return AddRelation(declaration);
  }

 private:
  Result<std::vector<std::unique_ptr<substrait::Rel>>> GetJoinRelationFromDeclaration(
      const compute::Declaration declaration, ExtensionSet* ext_set) {
    auto declr_left = declaration.inputs[0];
    auto declr_right = declaration.inputs[1];
    // TODO: figure out a better way
    if (util::get_if<compute::ExecNode*>(&declr_left) ||
        util::get_if<compute::ExecNode*>(&declr_right)) {
      return Status::NotImplemented("Only support Plans written in Declaration format.");
    }
    std::vector<std::unique_ptr<substrait::Rel>> rels;
    ARROW_ASSIGN_OR_RAISE(auto left_rel,
                          ToProto(util::get<compute::Declaration>(declr_left), ext_set));
    ARROW_ASSIGN_OR_RAISE(auto right_rel,
                          ToProto(util::get<compute::Declaration>(declr_right), ext_set));
    rels.emplace_back(std::move(left_rel));
    rels.emplace_back(std::move(right_rel));
    return rels;
  }
  substrait::Rel* rel_;
  ExtensionSet* ext_set_;
};

}  // namespace

Result<std::unique_ptr<substrait::Rel>> ToProto(const compute::Declaration& declaration,
                                                ExtensionSet* ext_set) {
  auto out = internal::make_unique<substrait::Rel>();
  RETURN_NOT_OK(ExtractRelation(out.get(), ext_set)(declaration));
  return std::move(out);
}

}  // namespace engine
}  // namespace arrow
