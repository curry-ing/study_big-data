# 6. 일괄처리 계층

> 학습목표
- 일괄처리 계층의 함수 계산
- 질의 분리(사전계산 vs 즉석계산)
- 재계산 알고리즘 vs 증분 알고리즘
- 확장성의 의미
- 맵리듀스의 패러다임과 상위 수준의 사고체계

데이터 시스템의 목적: 사용자의 질의에 적절히 응답하는 것
- 모든 질의에 대한 답을 마스터 데이터 셋 전체에서 찾기엔 **적절치** 않다 (질의 시간, 메모리)
- 자주 질의되는 부분에 대한 캐싱이 필요 -> **일괄처리 뷰**


## 6.1 일괄처리의 구실로 좋은 예제
정의역은 마스터 데이터 셋 전체, 요청시 즉석 계산이 아닌 사전 계산을 사용

### 6.1.1 시간대별 페이지뷰
목적: 지정한 시간대에서 발생한 특정 URL에 대한 페이지뷰 수의 총계
```
function pageviewsOverTime(matserDataset, url, startHour, endHour) {
  pageviews = 0
  for (record in masterDataset) {
    if (record.url == url && record.time >= startHour && reocrd.time <= endHour) {
      pageviews += 1
    }
  }
  return pageviews
}
```

### 6.1.2 성별 추론
목적: 이름 데이터 집합 레코드를 사용하여 개인의 성별 추론
- 고객 이름의 의미적 정규화(bob -> robert, bill -> william)
- 각 이름에 대한 성별의 확률을 적용
```
function genderInterface(masterDataset, personId) {
  names = new Set()
  for (record in masterDataset) {
    if (record.personId == personId) {
      names.add(normalizeName(record.name))
    }
  }
  mailProbSum = 0.0
  for (name in names) {
    maleProbSum += maleProbabilityOfName(name)
  }
  maleProb = maleProbSum / names.size()

  if (maleProbSum > 0.5) {
    return "male"
  } else {
    return "female"
  }
}
```
- 이름 정규화 알고리즘(`normalizeName`)이나, 이름으로 성별을 찾는 모델(`maleProbabilityOfName`)이 변경될 때에도 결과가 달라질 수 있다

### 6.1.3 영향력 지수
- 리액션 레코드: `sourceId`와 `responderId`를 기록
- 위 레코드를 사용하여 개인별 **영향력** 지수를 도출(`topKey`)
  - 반응 개수를 기반으로 각 개인에게 가장 큰 영향을 준 사람을 선택
  - 영향력 지수를 이 사람이 가장 큰 영향을 준 사람이 되는 사람들의 수로 결정

```
function influence_score(masterDataset, personId) {
  influence = new Map()
  for (record in masterDataset) {
    curr =influence.get(record.responderId) || new Map(default=0)
    curr[record.sourceId] += 1
    influence.set(record.responderId, curr)
  }

  score = 0
  for (entry in influence) {
    if (topKey(entry.value) == personId) {
      score += 1
    }
  }
  return score
}
```

## 6.2 일괄처리 계층에서 계산을 수행하기
- 일괄처리 계층은 처리가 오래 걸려 지연시간이 길지만, 이는 속도 계층에서 보완
- 마스터 데이터 셋에서 바로 질의 처리 자체가 가능은 하지만 이를 더 효율을 높이기 위해 **일괄처리 뷰** 생성
**서빙 레이어에서의 계산을 예측 가능한 범위로 좁히는 작업**

## 6.3 재계산 알고리즘 vs 증분 알고리즘
증분 알고리즘이 성능적으로 더 효율적인듯 하지만 인적 내결함성, 알고리즘의 일반성 등에서 장단점이 있다

### 6.3.1 성능
- **CPU**: 증분 계산은 새로 들어온 데이터만큼만 처리하지만, 재계산은 전체에 대해 다시 계산
- **저장공간**: 증분 가능한 형태로 저장하기 위해서 필요한 자원이 증가
  - ex> 순 방문자 수 계산: 증분 계산에서는 순 방문자 여부를 판단하기 위해 해당 페이지에 접속한 고객 식별자를 모두 저장해야 한다

### 6.3.2 인적 내결함성
코드에 사람의 실수가 가미되어 데이터가 오염되었을 때...
- 재계산 알고리즘: 코드를 수정 배포하여 전체에 계산을 수행하면 완료
- 증분 알고리즘: 낱낱이 말하지 않아도 복잡하다! 그리고 그 작업 과정 자체를 신뢰할 수 있을까?

### 6.3.3 알고리즘의 일반성
- 증분 알고리즘이 성능이 좋지만 부대비용이 많이 발생 

### 6.3.4 알고리즘의 방식 선택하기
