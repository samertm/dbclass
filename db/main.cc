#include <algorithm>
#include <iostream>
#include <vector>
#include <tuple>
#include <unordered_map>
#include <cstdio>
#include <stdexcept>

#include "absl/strings/ascii.h"

extern "C" {
#include "thirdparty/csv_parser/csv.h"
}

using std::vector;
using std::tuple;
using std::get;
using std::string;
using std::cout;
using std::size_t;
using std::unordered_map;
using std::runtime_error;

class row_tuple {
 public:
  row_tuple() : row_data({}) {}
  row_tuple(unordered_map<string, string> row_data) :
      row_data(std::move(row_data)) {}

  unordered_map<string, string> row_data;
};

inline bool operator==(const row_tuple& lhs, const row_tuple& rhs){
  return lhs.row_data == rhs.row_data;
}
inline bool operator!=(const row_tuple& lhs, const row_tuple& rhs){ return !(lhs == rhs); }


auto EOF_tuple = row_tuple();

class iterator {
 public:
  virtual void init() = 0;
  virtual row_tuple next() = 0;
  virtual void close() = 0;
};

const int kMaxCSVLineLength = 100000;

class csv_scan_iterator : public iterator {
 public:
  csv_scan_iterator (string path, vector<string> headers) :
      path(path), headers(headers) {}

  void init() {
    // Read the headers from the first line of the CSV
    auto fp = std::fopen(this->path.c_str(), "r");
    if (fp == nullptr) {
      throw runtime_error("Could not open CSV with path: " + this->path);
    }
    this->fp = fp;

    vector<size_t> headers_to_csv_cols;
    int done = 0;
    int err = 0;
    char* line = fread_csv_line(fp, kMaxCSVLineLength, &done, &err);
    if (done) {
      throw runtime_error("CSV has no data: " + this->path);
    }
    if (err) {
      throw runtime_error("CSV reading error: " + this->path);
    }
    char **parsed = parse_csv(line); // TODO check null/error?
    free(line);
    char **parsed_start = parsed;

    // Get all headers from the csv
    vector<string> csv_headers;
    for ( ; *parsed != nullptr ; parsed++ ) {
      auto s = string(*parsed);
      absl::AsciiStrToLower(&s);
      csv_headers.push_back(s);
    }

    for (const auto& h : headers) {
      auto found = false;
      // This is O(n^2), but n should be small (hopefully smaller
      // than the overhead of creating a map?).
      for (size_t i = 0; i < csv_headers.size(); i++) {
        auto ch = csv_headers[i];
        if (h == ch) {
          found = true;
          headers_to_csv_cols.push_back(i);
          break;
        }
      }
      if (!found) {
        throw runtime_error("Could not find header: " + h + "\n");
      }
    }
    this->headers_to_csv_cols = headers_to_csv_cols;

    free_csv_line(parsed_start);
  }

  row_tuple next() {
    if (this->is_done) {
      return EOF_tuple;
    }
    int done = 0;
    int err = 0;
    char *line = fread_csv_line(this->fp, kMaxCSVLineLength, &done, &err);
    if (done) {
      this->is_done = true;
      return EOF_tuple;
    }
    if (err) {
      throw runtime_error("CSV read failed with error: " + err);
    }
    char **parsed = parse_csv(line); // TODO check null/error?
    free(line);
    unordered_map<string, string> row_tuple_data;
    for (size_t i = 0; i < this->headers.size(); i++) {
      auto h = this->headers[i];
      auto csv_index = this->headers_to_csv_cols[i];
      row_tuple_data[h] = string(*(parsed + csv_index));
    }
    free_csv_line(parsed);
    return row_tuple(row_tuple_data);
  }

  void close() {
    std::fclose(this->fp);
    this->fp = nullptr;
    this->is_done = false;
    this->headers_to_csv_cols = {};
  }

 private:
  string path;
  vector<string> headers;
  FILE *fp;
  vector<size_t> headers_to_csv_cols;

  bool is_done = false;
};

class manual_tuple_scan_iterator : public iterator {
 public:
  manual_tuple_scan_iterator (vector<row_tuple> rows) :
      rows(rows) {}

  void init() {}

  row_tuple next() {
    if (this->rows_index >= this->rows.size()) {
      return EOF_tuple;
    }

    auto next_tuple = this->rows[this->rows_index];
    this->rows_index++;

    return next_tuple;
  }

  void close() {
    this->rows_index = 0;
  }

 private:
  vector<row_tuple> rows;
  std::size_t rows_index = 0;
};

class selection_iterator : public iterator {
 public:
  selection_iterator (iterator *input, bool (*predicate)(row_tuple)) :
      input(input), predicate(predicate) {}

  void init() {
    this->input->init();
  }

  row_tuple next() {
    row_tuple t;

    while ( (t = this->input->next()) != EOF_tuple ) {
      if (this->predicate(t)) {
        return t;
      }
    }

    return EOF_tuple;
  }

  void close() {
    this->input->close();
  }

 private:
  iterator *input;
  bool (*predicate)(row_tuple);
};

class projection_iterator : public iterator {
 public:
  projection_iterator (iterator *input, vector<string> cols_to_project) :
      input(input), cols_to_project(cols_to_project) {}

  void init() {
    this->input->init();
  }

  row_tuple next() {
    row_tuple t = this->input->next();
    if (t == EOF_tuple) {
      return EOF_tuple;
    }

    unordered_map<string, string> row_tuple_data;
    for (const auto& col_name : this->cols_to_project) {
      row_tuple_data[col_name] = t.row_data[col_name];
    }

    return row_tuple(row_tuple_data);
  }

  void close() {
    this->input->close();
  }

 private:
  iterator *input;
  vector<string> cols_to_project;
};

class average_iterator : public iterator {
 public:
  average_iterator (iterator *input, string col_to_average, string aggregated_col_name = "average") :
      input(input), col_to_average(col_to_average), aggregated_col_name(aggregated_col_name) {}

  void init() {
    this->input->init();
  }

  row_tuple next() {
    if (done) {
      return EOF_tuple;
    }

    row_tuple t;
    int count = 0;
    double sum = 0;
    while ( (t = this->input->next()) != EOF_tuple) {
      count++;
      double data_as_double = std::stod(t.row_data[this->col_to_average]);
      sum += data_as_double; // TODO check for overflows
    }

    double avg = sum / static_cast<double>(count);

    unordered_map<string, string> row_tuple_data =
        {{this->aggregated_col_name, std::to_string(avg)}};

    done = true;
    return row_tuple(row_tuple_data);
  }

  void close() {
    this->done = false;
    this->input->close();
  }

 private:
  iterator *input;
  string col_to_average;
  bool done = false;
  string aggregated_col_name;
};

class sort_iterator : public iterator {
 public:
  // Pass `col_to_sort: ""` to sort on all rows.
  sort_iterator (iterator *input, string col_to_sort) :
      input(input), col_to_sort(col_to_sort) {}

  void init() {
    this->input->init();

    // Read all data into memory.
    auto rows = new vector<row_tuple>();
    row_tuple t;
    while ( (t = this->input->next()) != EOF_tuple) {
      rows->push_back(t);
    }

    auto col_to_sort = this->col_to_sort;

    std::sort(rows->begin(), rows->end(), [&col_to_sort](row_tuple &a, row_tuple &b) {
        // TODO: use const args
        if (col_to_sort != "") {
          return a.row_data[col_to_sort] < b.row_data[col_to_sort];
        }
        for ( const auto& n : a.row_data ) {
          if (n.second < b.row_data[n.first]) {
            return true;
          } else if (n.second > b.row_data[n.first]) {
            return false;
          }
          // if the values are equal, check the new col
        }
        // If the rows are equal, default to false
        return false;
      });

    this->sorted_rows = rows;
  }

  row_tuple next() {
    if (this->index >= this->sorted_rows->size()) {
      return EOF_tuple;
    }

    row_tuple next_tuple = (*this->sorted_rows)[this->index];
    this->index++;
    return next_tuple;
  }

  void close() {
    delete this->sorted_rows;
    this->sorted_rows = nullptr;
    this->index = 0;
    this->input->close();
  }
 private:
  iterator *input;
  string col_to_sort;
  vector<row_tuple> *sorted_rows = nullptr;
  size_t index = 0;
};

class distinct_iterator : public iterator {
 public:
  distinct_iterator (iterator *input) :
      input(input) {}

  void init() {
    this->input->init();
  }

  row_tuple next() {
    if (this->done) {
      return EOF_tuple;
    }

    row_tuple t;
    while ( (t = this->input->next()) != EOF_tuple) {
      if (first || (this->current_row != t)) {
        this->current_row = t;
        first = false;
        return this->current_row;
      }
    }
    this->done = true;
    return EOF_tuple;
  }

  void close() {
    this->done = false;
    this->first = true;
    this->current_row = row_tuple();
    this->input->close();
  }

 private:
  iterator *input;
  row_tuple current_row;
  bool first = true;
  bool done = false;
};


void print_data(iterator *it) {
  it->init();
  row_tuple t;

  while ( (t = it->next()) != EOF_tuple ) {
    bool first = true;
    for ( const auto& n : t.row_data ) {
      if (first) {
        first = false;
      } else {
        cout << ", ";
      }
      cout << n.first << ": " << n.second;
    }
    cout << "\n";
  }

  it->close();
}

void test_movies_csv() {
  auto s = csv_scan_iterator("/home/samer/src/db/resources/movielens/movies.csv", {"movieid", "title"});

  auto selection_node = selection_iterator(&s, [](row_tuple t) -> bool {
      return t.row_data["movieid"] == "24";
    });

  auto projection_node = projection_iterator(&selection_node, {"title"});

  print_data(&projection_node);
}

void test_average_iterator() {
  auto m_node = manual_tuple_scan_iterator({
      row_tuple({{"name", "samer"}, {"age", "11.5"}}),
          row_tuple({{"name", "john"}, {"age", "30"}}),
          row_tuple({{"name", "fred"}, {"age", "20"}}),
          row_tuple({{"name", "my grandmother"}, {"age", "110.1"}})
    });

  auto a_node = average_iterator(&m_node, "age");

  print_data(&a_node);
}

void test_ratings_csv() {
  auto cs_node = csv_scan_iterator("/home/samer/src/db/resources/movielens/ratings-100.csv", {"movieid", "rating"});

  auto s_node = selection_iterator(&cs_node, [](row_tuple t) -> bool {
      return t.row_data["movieid"] == "1222";
    });

  auto a_node = average_iterator(&s_node, "rating");

  print_data(&a_node);
}

void test_sort_iterator() {
  auto m_node = manual_tuple_scan_iterator({
      row_tuple({{"name", "samer"}, {"age", "11.5"}}),
          row_tuple({{"name", "john"}, {"age", "30"}}),
          row_tuple({{"name", "fred"}, {"age", "20"}}),
          row_tuple({{"name", "my grandmother"}, {"age", "110.1"}})
    });

  auto s_node = sort_iterator(&m_node, "name");

  print_data(&s_node);
}

void test_distinct_iterator() {
  auto m_node = manual_tuple_scan_iterator({
      row_tuple({{"name", "samer"}, {"age", "11.5"}}),
          row_tuple({{"name", "john"}, {"age", "30"}}),
          row_tuple({{"name", "john"}, {"age", "30"}}),
          row_tuple({{"name", "john"}, {"age", "30"}}),
          row_tuple({{"name", "fred"}, {"age", "20"}}),
          row_tuple({{"name", "my grandmother"}, {"age", "110.1"}})
    });

  auto s_node = sort_iterator(&m_node, "");

  auto d_node = distinct_iterator(&s_node);

  print_data(&d_node);
}

int main() {
  // test_movies_csv();
  // test_average_iterator();
  // test_ratings_csv();
  // test_sort_iterator();
  // test_distinct_iterator();
}
