#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <random>

template <typename TYPE,
          typename = typename std::enable_if<arrow::is_number_type<TYPE>::value |
                                             arrow::is_boolean_type<TYPE>::value |
                                             arrow::is_temporal_type<TYPE>::value>::type>
arrow::Result<std::shared_ptr<arrow::Array>> GetArrayDataSample(
    const std::vector<typename TYPE::c_type>& values) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  ARROW_ASSIGN_OR_RAISE(auto int64_array,
                        GetArrayDataSample<arrow::Int64Type>(
                            {1, 2, 3, 3, 4, 4, 5, 6, 7, 8}));
  ARROW_ASSIGN_OR_RAISE(auto float_array,
                        GetArrayDataSample<arrow::DoubleType>(
                            {0.1, 0.2, 0.3, 0.3, 0.4, 0.4, 0.5, 0.6, 0.7, 0.8}));

  auto record_batch =
      arrow::RecordBatch::Make(arrow::schema({arrow::field("a", arrow::int64()),
                                              arrow::field("b", arrow::boolean())}),
                               10, {int64_array, float_array});
  return arrow::Table::FromRecordBatches({record_batch});
}

arrow::Status DoSampleTable() {
  auto schema = arrow::schema({arrow::field("a", arrow::int32()), arrow::field("b", arrow::float64())});
  
  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());

  // Set the number of samples to take
  int num_samples = 5;

  // Generate a sequence of uniform indexes using a random number generator
  std::mt19937 rng(0);
  std::uniform_int_distribution<int> index_dist(0, static_cast<int>(table->num_rows()) - 1);
  std::vector<int> indexes(num_samples);
  for (int i = 0; i < num_samples; i++) {
    indexes[i] = index_dist(rng);
  }

  // Sort the index vector to improve Gather performance
  std::sort(indexes.begin(), indexes.end());

  // Create a selection vector from the index vector
  arrow::Int32Builder builder;
  std::shared_ptr<arrow::Int32Array> selection;
  ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int32_t>(indexes.size())));
  ARROW_RETURN_NOT_OK(builder.AppendValues(indexes));

  ARROW_RETURN_NOT_OK(builder.Finish(&selection));

  // Sample the input table using arrow::compute::Take
  ARROW_ASSIGN_OR_RAISE(auto sampled_datum, arrow::compute::Take(table, selection));

  auto sampled_table = sampled_datum.table();

  std::cout << sampled_table->ToString() << std::endl;
  return arrow::Status::OK();
}

int main() {
  // Create a table to sample from
  auto status = DoSampleTable();
  if(!status.ok()) {
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
