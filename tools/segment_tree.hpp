#include <iostream>
#include <tuple>
#include <vector>

using namespace std;

struct SegmentTree {
  struct TreeNode {
    int val;
  };

  SegmentTree(int _n) : n(1) {
    while (n < _n) {
      n <<= 1;
    }
    nodes.resize(n * 2);
  }

  std::tuple<int, int> getLastLargerThan(int val) { return _getLastLargerThan(1, 0, n, val); }

  std::tuple<int, int> _getLastLargerThan(int p, int l, int r, int val) {
    if (nodes[p].val < val) {
      return {-1, 0};
    }
    if (l + 1 == r) {
      return {l, nodes[p].val};
    }
    int m = (l + r) / 2;
    if (nodes[p * 2 + 1].val >= val) {
      return _getLastLargerThan(p * 2 + 1, m, r, val);
    } else {
      return _getLastLargerThan(p * 2, l, m, val);
    }
  }

  void update(int i, int val) { _update(1, 0, n, i, val); }

  void _update(int p, int l, int r, int i, int val) {
    if (l + 1 == r) {
      nodes[p].val = val;
      return;
    }
    int m = (l + r) / 2;
    if (i < m) {
      _update(p * 2, l, m, i, val);
    } else {
      _update(p * 2 + 1, m, r, i, val);
    }
    _pushUp(p);
  }

  void _pushUp(int p) { nodes[p].val = std::max(nodes[p * 2].val, nodes[p * 2 + 1].val); }

  int n;
  std::vector<TreeNode> nodes;
};
