#include <iostream>
#include <vector>
#include <tuple>
#include <unordered_map>

#include "thirdparty/fast-cpp-csv-parser/csv.h"

using std::vector;
using std::tuple;
using std::get;
using std::string;
using std::cout;
using std::size_t;
using std::unordered_map;

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

class csv_scan : public iterator {
 public:
  void init(string path) {
    this->path = path;
  }
  row_tuple next() {
  }
  void close() {
    // SAMER
  }

 private:
  string path;
}

class manual_tuple_scan : public iterator {
 public:
  void init() {
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

  void set_input(vector<row_tuple> rows) {
    this->rows = std::move(rows);
  }
 private:
  vector<row_tuple> rows;
  std::size_t rows_index = 0;
};

class selection : public iterator {
 public:
  void init(bool (*predicate)(row_tuple)) {
    this->predicate = predicate;
  }
  row_tuple next() {
    row_tuple t;

    while ( (t = this->it->next()) != EOF_tuple ) {
      if (this->predicate(t)) {
        return t;
      }
    }

    return EOF_tuple;
  }
  void close() {
  }

  void set_input(iterator *it) {
    this->it = it;
  }

 private:
  bool (*predicate)(row_tuple);
  iterator *it;
};

int main() {
  manual_tuple_scan s;
  s.set_input({
      row_tuple({{"name", "samer"}, {"age", "11"}}),
          row_tuple({{"name", "jake"}, {"age", "14"}}),
          row_tuple({{"name", "michael"}, {"age", "13"}}),
          row_tuple({{"name", "matthew"}, {"age", "14"}}),
          row_tuple({{"name", "cameron"}, {"age", "12"}}),
          row_tuple({{"name", "samer"}, {"age", "12"}}),
    });

  auto selection_node = selection();
  selection_node.init([](row_tuple t) -> bool {
      return t.row_data["name"] == "samer";
    });
  selection_node.set_input(&s);

  row_tuple t;

  while ( (t = selection_node.next()) != EOF_tuple ) {
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
