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
  virtual row_tuple next() = 0;
  virtual void close() = 0;
};

const int kMaxCSVLineLength = 100000;

class csv_scan : public iterator {
 public:
  void init(string path, vector<string> headers) {
    this->path = path;
    this->headers = headers;

    // Read the headers from the first line of the CSV
    auto fp = std::fopen(this->path.c_str(), "r");
    if (fp == NULL) {
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
    char **parsed_start = parsed;

    // Get all headers from the csv
    vector<string> csv_headers;
    for ( ; *parsed != NULL ; parsed++ ) {
      auto s = string(*parsed);
      absl::AsciiStrToLower(&s);
      csv_headers.push_back(s);
    }

    for (auto h : headers) {
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
    // TODO
  }

 private:
  string path;
  vector<string> headers;
  FILE *fp;
  vector<size_t> headers_to_csv_cols;

  bool is_done = false;
};

class manual_tuple_scan : public iterator {
 public:
  void init(vector<row_tuple> rows) {
    this->rows = std::move(rows);
  }
  row_tuple next() {
    if (this->rows_index >= this->rows.size()) {
      return EOF_tuple;
    }

    auto next_tuple = this->rows[this->rows_index];
    this->rows_index++;

    return next_tuple;
  }
  void close() {
  }

 private:
  vector<row_tuple> rows;
  std::size_t rows_index = 0;
};

class selection : public iterator {
 public:
  void init(iterator *input, bool (*predicate)(row_tuple)) {
    this->input = input;
    this->predicate = predicate;
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
  }

 private:
  bool (*predicate)(row_tuple);
  iterator *input;
};

class projection : public iterator {
 public:
  void init(iterator *input, vector<string> cols_to_project) {
    this->input = input;
    this->cols_to_project = cols_to_project;
  }
  row_tuple next() {
    row_tuple t = this->input->next();
    if (t == EOF_tuple) {
      return EOF_tuple;
    }

    unordered_map<string, string> row_tuple_data;
    for (auto col_name : this->cols_to_project) {
      row_tuple_data[col_name] = t.row_data[col_name];
    }

    return row_tuple(row_tuple_data);
  }
  void close() {
  }

 private:
  iterator *input;
  vector<string> cols_to_project;
};



int main() {
  csv_scan s;
  s.init("/home/samer/src/db/resources/movielens/movies.csv", {"movieid", "title"});

  auto selection_node = selection();
  selection_node.init(&s, [](row_tuple t) -> bool {
      return t.row_data["movieid"] == "24";
    });

  auto projection_node = projection();
  projection_node.init(&selection_node, {"title"});

  row_tuple t;

  while ( (t = projection_node.next()) != EOF_tuple ) {
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

  return 0;
}
