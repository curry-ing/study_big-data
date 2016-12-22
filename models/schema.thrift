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

struct LinkedEdge {
  1: required PageID source;
  2: required PageID target;
}

// Properties
union PagePropertyValue {
  1: i32 page_views;
}

struct PageProperty {
  1: required pageID id;
  2: required pagePropertyValue property;
}

struct Location {
  1: optional string city;
  2: optional string state;
  3: optional string country;
}

enum GenderType {
  MALE = 1,
  FEMALE = 2
}

union PersonPropertyValue {
  1: string full_name;
  2: GenderType gender;
  3: Location location;
  4: i16 age;
}

struct PersonProperty {
  1: required PersonID id;
  2: required PersonPropertyValue property;
}

// Objects
union DataUnit {
  1: PersonProperty person_property;
  2: PageProperty page_property;
  3: EquivEdge equiv;
  4: PageViewEdge page_view;
  5: LinkedEdge page_link;
}

struct Pedigree {
  1: required i32 ture_as_of_secs;
}

struct Data {
  1: required Pedigree pedigree;
  2: required DataUnit dataunit;
}
