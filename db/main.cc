#include <iostream>
#include <vector>
#include <tuple>

using std::vector;
using std::tuple;
using std::get;
using std::string;
using std::cout;
using std::size_t;

class row_tuple {
 public:
  row_tuple() : row_names({}), row_data({}) {}
  row_tuple(vector<string> row_names, vector<string> row_data) :
      row_names(std::move(row_names)), row_data(std::move(row_data)) {}

  // TODO: reimplement using unordered_map
  vector<string> row_names;
  vector<string> row_data;
};

inline bool operator==(const row_tuple& lhs, const row_tuple& rhs){
  if (lhs.row_names.size() != rhs.row_names.size() ||
      lhs.row_data.size() != rhs.row_data.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.row_names.size(); i++) {
    if (lhs.row_names[i] != rhs.row_names[i]) {
      return false;
    }
  }
  for (size_t i = 0; i < lhs.row_data.size(); i++) {
    if (lhs.row_data[i] != rhs.row_data[i]) {
      return false;
    }
  }
  return true;
}
inline bool operator!=(const row_tuple& lhs, const row_tuple& rhs){ return !(lhs == rhs); }


auto EOF_tuple = row_tuple({}, {});

class iterator {
 public:
  virtual row_tuple next() = 0;
  virtual void close() = 0;
};

class scan : public iterator {
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
  auto s = scan();
  s.set_input({
      row_tuple({"name", "age"}, {"samer", "11"}),
          row_tuple({"name", "age"}, {"jake", "14"}),
          row_tuple({"name", "age"}, {"michael", "13"}),
          row_tuple({"name", "age"}, {"matthew", "14"}),
          row_tuple({"name", "age"}, {"cameron", "12"}),
          row_tuple({"name", "age"}, {"samer", "12"}),
    });

  auto selection_node = selection();
  selection_node.init([](row_tuple t) -> bool {
      return t.row_data[0] == "samer";
    });
  selection_node.set_input(&s);

  row_tuple t;

  while ( (t = selection_node.next()) != EOF_tuple ) {
    for (size_t i = 0; i < t.row_names.size(); i++) {
      if (i > 0) {
        cout << ", ";
      }
      cout << t.row_names[i] << ": " << t.row_data[i];
    }
    cout << "\n";
  }

  return 0;
}
