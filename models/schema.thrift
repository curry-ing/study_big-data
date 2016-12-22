// Nodes
union PersonID {
  1: string cookie;
  2: i64 user_id;
}

union PageID {
  1: string url;
}

// Edges
struct EquivEdge {
  1: required PersonID id1;
  2: required PersonID id2;
}

struct PageViewEdge {
  1: required PersonID person;
  2: required PageID page;
  3: required i64 nonce;
}
