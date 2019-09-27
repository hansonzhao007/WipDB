#include <vector>
#include <deque>
using namespace std;



        a0
    b1       c1
d2    e   g2    f2
        h3  i


struct Node {
    Node* parent;
    vector<Node*> children;
    int val;
};

Node* FindCommonParent(Node* a, Node* b) {
    if (!a || !b) {
        return nullptr;
    }

    vector<Node*> path_a, path_b;

    Parent(a, path_a);
    Parent(b, path_b);

    int len = min(path_a.size(), path_b.size());


    for(int = 0; i < len; ++i) {
        if (path_a[i] != path_b[i]) {
            if (i == 0) return nullptr;
            return path_a[i-1];
        }
    }

    return nullptr; // a and b is not in the same tree
}

void Parents(Node* node, vector<Node*>& path) {
    if (node == nullptr) return;
    while (node) {
        path.push_back(node);
        node = node->parent;
    }
}
