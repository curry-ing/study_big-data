# 3. 빅데이터를 위한 데이터 모델: 사례

> 학습목표
- 아파치 스리프트
- 아파치 스리프트를 사용한 그래프 스키마 구현
- 직렬화 프레임워크의 한계

## 3.1 어째서 직렬화 프레임워크인가?

#### Schema vs Schemaless
- **schema-less**: 단순하여 쉽게 접근할 수 있지만, 쉽게 오염될 수 있다 (json 등)
- **schema**: 데이터가 강건하게 유지(잘못된 데이터 입력시 Exception) (rdbms의 테이블 구조?)

#### 직렬화 프레임워크
- 강제가능 스키마를 쉽게 적용 가능
- 객체를 스키마에 맞춰 읽고, 쓰고, 유효성을 검증하는 코드를 원하는 코드로 생성
- 한계: 완전히 엄격한 스키마를 달성 불가


## 3.2 아파치 스리프트
- 인터페이스 정의 언어(interface definition language)로 정적 타입의 강제가능 스키마를 정의
- 이종 언어간 인터페이스를 위해 개발되었으나 현재는 여러 목적으로 사용됨

#### 주요 구성요소
구조체(struct)와 공용체(union) 타입 정의
- 기본 데이터 타입(string, int, long, double)
- 컬렉션(List, Map, Set)
- 다른 구조체와 공용체

Node: 공용체, Edge: 구조체, Property: 이들의 조합

### 3.2.1 Node
- **공용체**(union) 에 적합
  - 하나의 값으로 여러가지를 나타내는 타입 (물론, 한 가지만 나타내는 노드에도 적합)
  - 사용자 node에서 개인은 사용자 ID나 쿠키로 식별 가능하나 동시에 식별에 사용되지는 않음
  - 데이터 발전에 따른 스키마의 발전이 허용

##### 스리프트 공용체를 사용하여 `SuperWebAnalytics.com`중 일부 노드 정의  
```thrift
union PersonID {
  1: string cookie;
  2: i64 user_id;
}

union PageID {
  1: string url;
}
```

### 3.2.2 Edge
- **구조체**(struct) 에 적합
  - 간선은 두 개의 노드를 포함하는 구조체로 표현
  - 이름: 그것이 표현하는 관계
  - 필드: 그 관계로 엮인 개체

- `required` vs `optional`
  - `required`: 해당 필드에 값이 반드시 제공되어야 함(값이 없으면 스리프트단에서 직렬화, 역직렬화 시 에러 발생)
  - `optional`: `required`의 반대

```thrift
struct EquivEdge {
  1: required PersonID id1;
  2: required PersonID id2
}

struct PageViewEdge {
  1: required PersonID person;
  2: required PageID page;
  3: required i64 nonce;
}
```

### 3.2.3 Property
- Property는 노드와 각 노드에 대한 속성값을 가진다
- 속성값은 여러 타입이 될 수 있으므로 공용체가 유리

##### 페이지 속성에 사용할 스키마 정의
```thrift
union PagePropertyValue {
  1: i32 page_views;
}

struct PageProperty {
  1: required PageID id;
  2: required PagePropertyValue property;
}
```

##### 유저 속성 정의(주거지 속성이 복잡하여 구조체가 하나 추가)
```thrift
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
}

struct PersonProperty {
  1: required PersonID id;
  2: required PersonPropertyValue property;
}
```
- 주거지 구조체  
  - 시, 주, 국가는 따로 존재해도 무방
  - 각 필드가 긴밀하기 때문에 이들을 모두 하나의 구조체 내에 `optional`로 선언

### 3.2.4 노드, 간선, 속성을 모두 엮어 데이터 객체로 만들기
#### `DataUnit` 공용체
- 별도로 정의된 데이터를 엮어 단일한 인터페이스 제공
- 데이터 관리도 용이

##### `SuperWebAnalytics.com` 스키마 완성
```thrift
union DataUnit {
  1: PersonProperty person_property;
  2: PageProperty page_property;
  3: EquivEdge equiv;
  4: PageViewEdge page_view;
}

struct Pedigree {
  1: required i32 ture_as_of_secs;
}

struct Data {
  1: required Pedigree pedigree;
  2: required DataUnit dataunit;
}
```
- 각 `DataUnit`은 `Pedigree` 구조체에 저장된 메타 데이터와 매치
- 현재는 타임스탬프만을 가지고 있으나, 필요시 디버깅 정보나 데이터의 출처를 포함 가능
- `Data`구조체는 팩트 기반 모델에서의 **팩트** 에 해당

[`SuperWebAnalytics.com` 스키마](../../models/schema.thrift)

### 3.2.5 스키마 발전시키기
- 스리프트는 수시로 스키마를 발전시킬 수 있도록 설계
- 비즈니스의 요구사항이 변경될 때 마다 쉽게 대응 가능
- 각 필드에 연결된 숫자 식별자가 쉬운 업데이트의 열쇠 (직렬화된 형식으로 필드를 식별)

#### 스키마 변경시 호환성 유지를 위한 규칙
- **필드명은 변경해도 무방**: 식별자로 ID를 사용하기 때문
- **필드 삭제시 해당 id 재사용 금지**
  - 기존 데이터 역직렬화시 스리프트는 스키마에 포함하지 않는 ID는 모두 무시
  - 삭제된 ID를 재사용시 스리프트는 오래된 데이터를 새로운 필드로 역질렬화 시도
    -> 이 때문에 유효하지 않거나, 잘못된 데이터가 생성 가능
- **기존 구조체에는 `optional`만 추가 가능**
  - `required`가 추가되면 기존 데이터에는 그 필드가 없을 터인데 결국 역직렬화 불가능
  - 공용체에는 `required`나 `optional`이 없기 때문에 해당사항 없음

##### `SuperWebAnalytics.com`스키마 발전
[`SuperWebAnalytics.com` update](https://github.com/masunghoon/study_big-data/commit/ef6df74127591be18c3c561809c06b08b1eadcec#diff-4114df5a2e8be2296509e296a2570a0f)
- 사람의 나이와 웹페이지 사이의 링크를 저장
  - `PersonPropertyValue`에 나이 필드 추가
  - `LinkedEdge`라는 엣지를 생성하고 `DataUnit`에 해당 간선 추가


## 3.3 직렬화 프레임워크의 한계

#### Validation Check 의 어려움
- `required`점검 외 추가적인 유효성 검증이 어려움
  -> 스키마는 데이터를 받아 유효 여부를 반환하는 하나의 함수로 간주

#### 추가 세부사항 검증 대안
1. Validation 코드를 직렬화 프레임워크가 생성한 코드에 Wrapping
  - 단일 언어를 사용하면 심플하나 여러 언어를 사용하는 경우 파편화 & 유지보수 힘듦
2. 일괄처리 작업의 시작점에서 valid-check
  - 처리 로직 시작단계에서 valid/invalid 데이터를 분리하여 유효하지 않은 데이터 발견 시 alert
  - 오염된 데이터가 마스터 데이터 집합에 쓰여지는 것 자체를 차단 불가(전후 상황 파악도 불가능하여 추후 재발 가능)

완벽한 대응 방안은 현실적으로 존재하기 힘들고 위의 1, 2번 대안 중 각자의 상황에 맞게 선택할 것


## 3.4 마무리

#### 배운 것
- 직렬화 프레임워크로 그래프 스키마 작성 및 직렬화 프레임워크의 한계
- 하지만 어떤것이 한계인지를 인지하는것이 중요

#### 배울 것
- **쉽고 효율적인 처리를 위해** batch-layer에 마스터 데이터셋을 저장하는 방법 (물리적으로)
