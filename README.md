# Flink CDC Admin Tool

웹 UI를 통해 사용자가 작성한 Flink CDC Pipeline YAML을 Kubernetes 클러스터에 쉽게 배포하고 관리하는 도구.

---

## 1. 프로젝트 개요

### 1.1 문제 정의

Flink CDC는 YAML 기반의 선언적 파이프라인 정의를 통해 데이터베이스 변경 사항을 실시간으로 캡처하고 다른 시스템에 동기화하는 강력한 도구다. Pipeline YAML 작성 자체는 직관적이지만, 이를 Kubernetes 환경에서 운영하려면 다음과 같은 **별도의 K8s 배포 작업**이 필요하다:

- ConfigMap 생성 및 마운트 설정
- FlinkDeployment CR 작성 (podTemplate, volume, args, classloader 설정 등)
- `kubectl apply` 실행
- 상태 모니터링을 위한 `kubectl get`/`describe` 반복

Pipeline YAML을 작성할 수 있는 사용자라도, Kubernetes 리소스를 직접 다루는 것은 번거롭고 실수가 발생하기 쉽다. 본 도구는 이 **"Pipeline YAML → K8s 배포" 사이의 간극**을 해소한다.

### 1.2 목표

- **K8s 배포 자동화**: 사용자가 제공한 Pipeline YAML과 Docker 이미지를 기반으로 ConfigMap + FlinkDeployment CR을 자동 생성하여 클러스터에 배포한다.
- **단순한 웹 UI**: Pipeline YAML 입력, 이미지 선택, 리소스 설정만으로 배포할 수 있는 UI를 제공한다.
- **상태 가시성**: 작업 목록, 실행 상태, 에러 정보를 한눈에 확인할 수 있다.
- **v0 집중**: 생성(Create) / 삭제(Delete) / 조회(Read) 기능에 집중한다.

### 1.3 전제

- **사용자는 Pipeline YAML을 직접 작성할 수 있다.** 소스/싱크 커넥터, 라우트, 트랜스폼 등의 설정은 사용자의 책임이다.
- **사용자는 적절한 Docker 이미지를 알고 있다.** 필요한 커넥터 JAR가 포함된 이미지를 선택하는 것은 사용자의 몫이다.
- Admin Tool은 Pipeline YAML의 내용을 해석하거나 검증하지 않는다. 그대로 ConfigMap에 담아 배포한다.

### 1.4 비목표

- Pipeline YAML의 구조를 분해하여 폼 필드로 추상화하는 것 (Pipeline 편집기가 아님)
- 커넥터별 필드 스키마 제공 및 동적 폼 생성
- Flink Kubernetes Operator의 설치/업그레이드 관리
- 멀티 클러스터 지원
- 멀티 테넌시, 조직 모델, 과금 시스템
- IDE급 파이프라인 편집기 (코드 자동완성, 문법 검증 등)
- 고급 관측 스택 구축 (Prometheus/Grafana 통합 대시보드)
- 클러스터/Operator 자동 프로비저닝

---

## 2. 전제조건 (Prerequisites)

본 시스템은 다음 조건이 충족된 환경에서만 동작한다:

### 필수 조건

| 항목 | 설명 |
|---|---|
| Kubernetes 클러스터 | 1.24+ 권장 |
| Flink Kubernetes Operator | v1.6+ 설치 완료 |
| FlinkDeployment CRD | 클러스터에 등록 완료 |
| Flink CDC Docker 이미지 | 필요한 커넥터 JAR가 포함된 이미지가 접근 가능한 레지스트리에 존재 |
| ServiceAccount | Flink 작업용 ServiceAccount 존재 (기본: `flink`) |

### 사전 점검 체크리스트

```bash
# 1. FlinkDeployment CRD 존재 확인
kubectl get crd flinkdeployments.flink.apache.org
# 예상 출력: flinkdeployments.flink.apache.org   <DATE>

# 2. Flink Kubernetes Operator Pod 정상 동작 확인
kubectl get pods -n flink-operator-system
# 예상 출력: flink-kubernetes-operator-xxxxx   1/1   Running

# 3. Admin App 서비스 계정 권한 점검
kubectl auth can-i create flinkdeployments --as=system:serviceaccount:<namespace>:<sa-name>

# 4. 샘플 FlinkDeployment 배포 검증
kubectl apply -f sample-flink-deployment.yaml
kubectl get flinkdeployment sample-job -w
# lifecycleState가 STABLE로 전이되면 정상
```

---

## 3. 사용자 시나리오

### 시나리오 1: MySQL → Kafka CDC 작업 생성

1. 사용자가 로컬에서 Pipeline YAML을 작성한다:
   ```yaml
   source:
     type: mysql
     hostname: mysql.prod.svc
     port: 3306
     username: cdc_user
     password: secret
     tables: app_db.\.*
     server-id: 5400-5404
   sink:
     type: kafka
     properties.bootstrap.servers: kafka:9092
   route:
     - source-table: app_db.orders
       sink-table: orders-topic
   pipeline:
     name: mysql-to-kafka-orders
     parallelism: 2
   ```
2. 웹 UI에 접속하여 "새 작업 만들기"를 클릭한다.
3. 폼에 다음을 입력한다:
   - **작업 이름**: `mysql-to-kafka-orders`
   - **Pipeline YAML**: 위 내용을 텍스트 영역에 붙여넣기
   - **Docker 이미지**: `my-registry/flink-cdc:3.3.0-mysql-kafka` (MySQL + Kafka 커넥터 포함)
   - **리소스 설정**: JobManager 1GB, TaskManager 2GB, parallelism 2
4. "배포" 버튼을 클릭한다.
5. 시스템이 ConfigMap(Pipeline YAML) + FlinkDeployment CR을 생성하고 클러스터에 적용한다.
6. 작업 목록에서 상태가 "배포중" → "실행중"으로 전이되는 것을 확인한다.

### 시나리오 2: 작업 삭제

1. 작업 목록에서 대상 작업을 선택한다.
2. "삭제" 버튼을 클릭한다.
3. 확인 다이얼로그 후 시스템이 FlinkDeployment 및 관련 ConfigMap을 삭제한다.
4. 작업이 목록에서 사라지거나 "삭제됨" 상태로 표시된다.

### 시나리오 3: 상태 조회

1. 작업 목록에서 각 작업의 현재 상태(배포중/실행중/실패 등)를 확인한다.
2. 특정 작업을 클릭하면 상세 정보를 볼 수 있다:
   - Flink Job 상태 및 ID
   - 최근 에러 메시지 (있는 경우)
   - Flink Web UI 링크
   - 생성 시각
   - 리소스 설정 요약
   - 원본 Pipeline YAML 확인

---

## 4. 기능 범위

### 4.1 MVP (v0)

| 기능 | 설명 |
|---|---|
| **작업 생성** | Pipeline YAML + Docker 이미지 + 리소스 설정 입력 → ConfigMap + FlinkDeployment CR 생성 → 클러스터 배포 |
| **작업 삭제** | FlinkDeployment 및 관련 리소스(ConfigMap) 정리 |
| **작업 목록 조회** | 전체 작업 리스트와 각 작업의 현재 상태 표시 |
| **작업 상세 조회** | 개별 작업의 상세 정보, Job 상태, 에러, Pipeline YAML 원본, Flink UI 링크 |

### 4.2 Future Scope (v1~v2)

구현하지 않지만, 아키텍처적으로 확장 가능해야 하는 기능:

| 기능 | 필요 API/리소스/권한 |
|---|---|
| **Savepoint 생성 트리거** | `FlinkStateSnapshot` CR 생성 권한, 또는 FlinkDeployment의 `savepointTriggerNonce` 패치 권한 |
| **Savepoint로부터 복구** | FlinkDeployment의 `spec.job.initialSavepointPath` + `savepointRedeployNonce` 패치 |
| **특정 Checkpoint로부터 복구** | Flink REST API (`/jobs/:jobId/checkpoints`) 조회 + `initialSavepointPath` 설정 |
| **Upgrade/재배포 전략** | FlinkDeployment spec 변경 + `upgradeMode` (`savepoint`/`last-state`) 설정, 롤백 설정 |

### 4.3 Out of Scope

다음은 설계에서 깊게 다루지 않는다:

- **멀티 테넌시 및 조직/과금 모델**: 사용자/조직 분리, 리소스 쿼터, 과금 통합은 범위 밖이다.
- **고급 관측 스택 구축**: 별도의 Prometheus/Grafana 대시보드 구축은 하지 않는다. 기본적인 상태 조회와 로그 접근만 제공한다.
- **IDE급 파이프라인 편집기**: 문법 하이라이팅, 자동완성, 실시간 검증이 포함된 에디터는 범위 밖이다. 사용자는 YAML을 직접 작성한다.
- **클러스터/Operator 자동 프로비저닝**: Kubernetes 클러스터나 Flink Operator의 자동 설치/관리는 하지 않는다.

---

## 5. 입력 모델 설계

### 5.1 설계 철학

Pipeline YAML의 내용(source, sink, route, transform, pipeline 섹션)은 사용자의 영역이다. Admin Tool은 이를 해석하거나 분해하지 않고, **그대로 ConfigMap에 담아 FlinkDeployment와 함께 배포하는 역할**에 집중한다.

사용자가 Admin Tool에 제공하는 정보는 크게 세 가지다:

1. **Pipeline YAML**: CDC 파이프라인 정의 (원본 그대로)
2. **Docker 이미지**: Pipeline에 필요한 커넥터 의존성이 포함된 이미지
3. **FlinkDeployment 설정**: K8s 리소스 할당 및 Flink 런타임 설정

### 5.2 UI 입력 필드

| 구분 | 필드 | 필수 | 타입 | 예시 값 | 설명 |
|---|---|---|---|---|---|
| **작업 정보** | `jobName` | Y | string | `mysql-to-kafka-orders` | 작업 이름 (FlinkDeployment name으로 사용) |
| **파이프라인** | `pipelineYaml` | Y | text | *(YAML 전문)* | 사용자가 작성한 CDC Pipeline YAML 원본 |
| **이미지** | `flinkImage` | Y | string | `my-registry/flink-cdc:3.3.0` | 사용할 Flink CDC Docker 이미지 (커넥터 JAR 포함) |
| **리소스** | `jobManager.cpu` | N | number | `1` | JM CPU (기본값: 1) |
| **리소스** | `jobManager.memory` | N | string | `1024m` | JM 메모리 (기본값: 1024m) |
| **리소스** | `taskManager.cpu` | N | number | `1` | TM CPU (기본값: 1) |
| **리소스** | `taskManager.memory` | N | string | `2048m` | TM 메모리 (기본값: 2048m) |
| **리소스** | `taskManager.replicas` | N | number | `2` | TM 레플리카 수 |
| **리소스** | `parallelism` | N | number | `2` | Job 병렬도 (기본값: 1) |

#### 고급 설정 (접힌 패널)

| 구분 | 필드 | 필수 | 타입 | 예시 값 | 설명 |
|---|---|---|---|---|---|
| **Flink** | `flinkVersion` | N | enum | `v1_18` | Flink 버전 (기본값: 설정 가능) |
| **Flink** | `serviceAccount` | N | string | `flink` | Flink ServiceAccount (기본값: `flink`) |
| **Flink** | `extraFlinkConfig` | N | key-value | `{"key":"value"}` | 추가 Flink 설정 (flinkConfiguration에 병합) |
| **배포** | `namespace` | N | string | `flink-jobs` | 배포 대상 네임스페이스 (기본값 사용 가능) |

### 5.3 Admin Tool의 역할 범위

```
┌─────────────────────────┐     ┌──────────────────────────────────┐
│     사용자 책임           │     │     Admin Tool 책임               │
│                         │     │                                  │
│  Pipeline YAML 작성      │     │  Pipeline YAML → ConfigMap       │
│  Docker 이미지 선택       │────▶│  FlinkDeployment CR 생성          │
│  리소스 요구사항 결정      │     │  K8s 리소스 배포/삭제             │
│                         │     │  상태 조회 및 표시                 │
│                         │     │  podTemplate/volume 자동 구성      │
│                         │     │  classloader 등 CDC 필수 설정      │
└─────────────────────────┘     └──────────────────────────────────┘
```

Admin Tool이 자동으로 처리하는 FlinkDeployment 설정 (사용자가 몰라도 되는 부분):
- `classloader.resolve-order: parent-first` (Flink CDC 필수)
- `podTemplate` 내 volume/volumeMount 구성 (ConfigMap 마운트)
- `job.jarURI`, `job.entryClass`, `job.args` 설정 (CDC CLI 진입점)
- Label/Annotation 부여
- ownerReferences 설정

---

## 6. Kubernetes 리소스 모델

### 6.1 생성되는 리소스 목록

하나의 Flink CDC 작업을 배포하면 다음 리소스가 생성된다:

| 리소스 | 용도 | 생성 주체 |
|---|---|---|
| `ConfigMap` | 사용자가 제공한 Pipeline YAML 저장 | Admin App |
| `FlinkDeployment` | Flink 클러스터 + Job 정의 | Admin App |
| JobManager Pod | Flink JobManager 실행 | Operator |
| TaskManager Pod(s) | Flink TaskManager 실행 | Operator |
| Service | JM REST/RPC 엔드포인트 | Operator |

> Admin App이 직접 생성하는 리소스는 ConfigMap과 FlinkDeployment이다.
> 나머지는 Operator가 FlinkDeployment를 관찰하여 자동 생성한다.

### 6.2 네이밍 전략

| 리소스 | 네이밍 패턴 | 예시 |
|---|---|---|
| FlinkDeployment | `{jobName}` | `mysql-to-kafka-orders` |
| ConfigMap | `{jobName}-pipeline` | `mysql-to-kafka-orders-pipeline` |

규칙:
- `jobName`은 Kubernetes 이름 규칙을 따른다 (소문자, 하이픈, 63자 이내).
- Admin App은 입력된 `jobName`을 slug화하여 사용한다.

### 6.3 Label 및 Annotation 전략

모든 Admin App이 생성하는 리소스에 공통 레이블을 부여한다:

```yaml
metadata:
  labels:
    app.kubernetes.io/managed-by: flink-cdc-admin
    app.kubernetes.io/name: {jobName}
    app.kubernetes.io/component: flink-cdc-pipeline
    flink-cdc-admin/version: v0
  annotations:
    flink-cdc-admin/created-at: "2026-02-13T12:00:00Z"
    flink-cdc-admin/created-by: "admin-user"
```

- `app.kubernetes.io/managed-by: flink-cdc-admin`: 이 레이블을 기준으로 Admin App이 관리하는 리소스를 필터링한다.
- 목록 조회 시 label selector로 활용: `app.kubernetes.io/managed-by=flink-cdc-admin`

### 6.4 ownerReferences 사용

FlinkDeployment를 ConfigMap의 owner로 설정한다:

```yaml
# ConfigMap의 ownerReferences 예시
metadata:
  ownerReferences:
    - apiVersion: flink.apache.org/v1beta1
      kind: FlinkDeployment
      name: mysql-to-kafka-orders
      uid: <FlinkDeployment UID>
      controller: true
      blockOwnerDeletion: true
```

효과:
- FlinkDeployment가 삭제되면 Kubernetes GC가 ConfigMap을 자동으로 정리한다.
- 별도의 정리 로직 없이 일관된 리소스 생명주기를 보장한다.

> **생성 순서**: ConfigMap 먼저 생성 → FlinkDeployment 생성 → ConfigMap에 ownerReferences 패치 (FlinkDeployment UID 참조)

---

## 7. 상태 모델 정의

### 7.1 UI 상위 상태

| UI 상태 | 설명 | 색상 제안 |
|---|---|---|
| **DEPLOYING** (배포중) | 리소스가 생성되었고 Operator가 처리 중 | 파란색 (회전) |
| **RUNNING** (실행중) | 파이프라인이 정상 실행 중 | 초록색 |
| **FAILED** (실패) | 작업이 실패한 상태 | 빨간색 |
| **SUSPENDED** (일시중지) | 작업이 의도적으로 중지된 상태 | 회색 |
| **UPGRADING** (업그레이드중) | 스펙 변경으로 재배포 중 | 파란색 |
| **UNKNOWN** (알 수 없음) | 상태를 판단할 수 없는 경우 | 검정색 |

### 7.2 Kubernetes 상태 → UI 상태 매핑

```
┌──────────────────────────────────────────────────────────────┐
│ Operator lifecycleState  │ jobStatus.state │ → UI 상태       │
├──────────────────────────┼─────────────────┼─────────────────┤
│ CREATED                  │ (any)           │ DEPLOYING       │
│ DEPLOYED                 │ CREATED         │ DEPLOYING       │
│ DEPLOYED                 │ RECONCILING     │ DEPLOYING       │
│ DEPLOYED                 │ RUNNING         │ DEPLOYING       │
│ STABLE                   │ RUNNING         │ RUNNING         │
│ STABLE                   │ FINISHED        │ RUNNING (완료)  │
│ SUSPENDED                │ (any)           │ SUSPENDED       │
│ UPGRADING                │ (any)           │ UPGRADING       │
│ ROLLING_BACK             │ (any)           │ UPGRADING       │
│ ROLLED_BACK              │ RUNNING         │ RUNNING (경고)  │
│ FAILED                   │ (any)           │ FAILED          │
│ (any)                    │ FAILED          │ FAILED          │
│ (any)                    │ FAILING         │ FAILED          │
│ (status 없음)            │ (status 없음)   │ UNKNOWN         │
└──────────────────────────────────────────────────────────────┘
```

### 7.3 상태 표현 방식

상태는 두 계층으로 표현한다:

1. **작업 수준 상태 (Primary)**: 위 표의 UI 상태. 목록 화면에 표시.
2. **상세 상태 (Secondary)**: 상세 화면에서 표시.
   - `lifecycleState` 원본 값
   - `jobStatus.state` 원본 값
   - `jobManagerDeploymentStatus` 값
   - `error` 메시지 (있는 경우)
   - 최근 reconciliation 결과

---

## 8. 보안 및 권한 모델

### 8.1 접근 방식

| 방식 | 설명 | v0 적용 여부 |
|---|---|---|
| **In-cluster** | Admin App이 클러스터 내 Pod으로 실행. ServiceAccount 토큰 자동 마운트. | **추천** |
| **Out-of-cluster** | Admin App이 클러스터 외부에서 실행. kubeconfig 파일 사용. | 로컬 개발용 |

**추천안**: v0에서는 in-cluster 배포를 기본으로 한다. 로컬 개발 시에는 kubeconfig를 통한 out-of-cluster 접근을 지원한다.

### 8.2 RBAC 설계

Admin App의 ServiceAccount에 필요한 최소 권한:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: flink-cdc-admin-role
  namespace: flink-jobs
rules:
  # FlinkDeployment 관리
  - apiGroups: ["flink.apache.org"]
    resources: ["flinkdeployments"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  # FlinkDeployment 상태 읽기
  - apiGroups: ["flink.apache.org"]
    resources: ["flinkdeployments/status"]
    verbs: ["get"]
  # ConfigMap 관리 (Pipeline YAML)
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "create", "update", "patch", "delete"]
  # Pod/Event 읽기 (상태 조회, 로그)
  - apiGroups: [""]
    resources: ["pods", "pods/log", "events"]
    verbs: ["get", "list", "watch"]
  # Service 읽기 (Flink UI 링크)
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list"]
```

### 8.3 민감 정보 처리

Pipeline YAML에는 소스/싱크 접속 비밀번호 등 민감 정보가 포함될 수 있다. 이에 대한 책임 분담:

| 관점 | 설명 |
|---|---|
| **사용자 책임** | Pipeline YAML 내 민감 정보 관리 방식은 사용자가 결정한다. |
| **v0 Admin Tool** | Pipeline YAML을 그대로 ConfigMap에 저장한다. ConfigMap은 etcd에 평문 저장되므로 K8s etcd 암호화 또는 네임스페이스 RBAC으로 보호한다. |
| **v1 확장** | Pipeline YAML 대신 Secret에 저장하거나, Flink CDC의 환경변수 참조 지원 여부에 따라 Secret 기반 주입을 구현할 수 있다. |

> v0에서는 Pipeline YAML의 민감 정보 분리를 강제하지 않는다. Kubernetes 네임스페이스 수준의 RBAC과 etcd encryption-at-rest로 보호하는 것을 전제로 한다.

---

## 9. 운영 및 관측

### 9.1 로그 접근 방식

| 로그 소스 | 접근 방법 | v0 지원 |
|---|---|---|
| JobManager 로그 | Kubernetes Pod 로그 API (`/api/v1/namespaces/{ns}/pods/{pod}/log`) | O |
| TaskManager 로그 | Kubernetes Pod 로그 API | O |
| Flink Web UI 로그 | Flink REST API 또는 Web UI 링크 제공 | 링크만 제공 |
| Admin App 자체 로그 | stdout/stderr (구조화 로깅) | O |

v0에서는 Flink Web UI 링크를 제공하여 사용자가 직접 상세 로그를 확인할 수 있도록 한다.

### 9.2 이벤트 활용

Kubernetes Events를 활용하여 작업 상태 변경 이력을 추적한다:

```bash
kubectl get events --field-selector involvedObject.name=mysql-to-kafka-orders
```

v0에서는 FlinkDeployment 관련 이벤트를 상세 화면에서 표시한다.

### 9.3 메트릭 확장 여지

v0에서는 메트릭 수집을 구현하지 않으나, 이후 확장을 위해 다음 포인트를 고려한다:

- Flink REST API의 `/jobs/:jobId/metrics` 엔드포인트 활용 가능
- Flink의 Prometheus metric reporter 설정을 `flinkConfiguration`에 추가하면 외부 모니터링 스택과 통합 가능
- Admin App 자체 메트릭 (작업 수, API 요청 수 등)은 `/metrics` 엔드포인트로 노출 가능

---

## 10. 확장 전략

### 10.1 Savepoint/Restore 추가 시 변경 포인트

| 변경 대상 | 내용 |
|---|---|
| **API 추가** | `POST /api/jobs/{id}/savepoint` (savepoint 트리거) |
| | `POST /api/jobs/{id}/restore` (savepoint/checkpoint로부터 복구) |
| **K8s 리소스** | `FlinkStateSnapshot` CR 생성 권한 추가 (RBAC) |
| **UI** | 작업 상세 화면에 "Savepoint 생성" 버튼, Savepoint 목록, "복구" 버튼 추가 |
| **상태 모델** | `SAVING`, `RESTORING` 상태 추가 |
| **저장** | Savepoint 경로 이력 관리 (DB 또는 annotation 활용) |

### 10.2 Upgrade 전략 확장 포인트

| 변경 대상 | 내용 |
|---|---|
| **API 추가** | `PUT /api/jobs/{id}` (작업 설정 업데이트) |
| **Backend** | FlinkDeployment spec 패치 로직 (이미지 변경, parallelism 변경 등) |
| **Backend** | Pipeline YAML 업데이트 시 ConfigMap 교체 |
| **UI** | "설정 변경" 폼 (새 Pipeline YAML, 이미지, 리소스), `upgradeMode` 선택 (`stateless`/`savepoint`/`last-state`) |
| **상태 모델** | `UPGRADING` 상태는 이미 정의됨. `ROLLING_BACK`, `ROLLED_BACK` 상태 UI 추가 |

---

## 11. 로컬 개발 전략

### 11.1 개발 환경 구성

```
┌─────────────────────────────────────────────┐
│  개발자 머신                                  │
│                                             │
│  ┌──────────┐     ┌──────────────────────┐  │
│  │ Frontend │────▶│   Backend (API)      │  │
│  │ dev      │     │   dev server         │  │
│  │ server   │     │                      │  │
│  └──────────┘     └──────────┬───────────┘  │
│                              │ kubeconfig   │
│                              ▼              │
│                   ┌──────────────────────┐  │
│                   │  K8s Cluster         │  │
│                   │  (kind / minikube /  │  │
│                   │   remote cluster)    │  │
│                   └──────────────────────┘  │
└─────────────────────────────────────────────┘
```

| 구성 요소 | 도구 | 용도 |
|---|---|---|
| 로컬 K8s | kind, minikube, Docker Desktop | 로컬 테스트용 클러스터 |
| Flink Operator | Helm 설치 | Operator 동작 확인 |
| Backend | 로컬 실행 (kubeconfig 참조) | API 개발 |
| Frontend | 로컬 dev server | UI 개발 |

### 11.2 개발 시 참고사항

- 로컬 클러스터에서는 실제 MySQL/Kafka 없이도 FlinkDeployment 생성 → Operator 처리 → 상태 관찰 흐름을 테스트할 수 있다 (Job이 실패하더라도 리소스 생성/상태 흐름은 동일).
- `kubectl port-forward`를 통해 Flink Web UI에 접근할 수 있다.
- 로컬 개발 시 Admin App이 out-of-cluster 모드로 동작하도록 kubeconfig 경로를 환경변수로 설정한다.

---

## 12. v0 추천 기술 스택

| 계층 | 기술 | 선택 근거 |
|---|---|---|
| **Frontend** | React + TypeScript | 넓은 생태계, 폼 처리 라이브러리 풍부 (React Hook Form 등) |
| **UI 프레임워크** | Ant Design 또는 shadcn/ui | 관리 도구에 적합한 테이블/폼/상태 컴포넌트 |
| **Backend** | Kotlin + Spring Boot 3 | Kubernetes Java Client 공식 지원, 풍부한 생태계, 빌드/배포 도구 성숙 |
| **K8s Client** | fabric8 Kubernetes Client | FlinkDeployment 등 CRD에 대한 타입 안전한 접근, Spring Boot 통합 용이 |
| **API 스타일** | REST (OpenAPI 3.0) | 단순한 CRUD에 적합, 코드 생성 도구 활용 가능 |
| **데이터 저장** | Kubernetes API (Source of Truth) | v0에서는 별도 DB 없이 K8s 리소스 자체를 데이터 소스로 사용 |
| **컨테이너화** | Docker + Jib (backend) | 빌드 파이프라인 단순화 |
| **로컬 K8s** | kind | 가볍고 CI에서도 사용 가능 |

### 기술 스택 선택 비교

#### Backend 언어/프레임워크

| 선택지 | 장점 | 단점 | 추천 |
|---|---|---|---|
| **Kotlin + Spring Boot** | K8s Java Client 성숙, 생태계 광범위, 타입 안전 | JVM 메모리, 이미지 크기 | **추천** |
| Go + Gin | 경량, K8s client-go 네이티브, 빠른 빌드 | Flink/CDC 생태계와 거리, 타입 시스템 한계 | 대안 |
| Node.js + NestJS | Frontend와 언어 통일 | K8s 클라이언트 생태계 미성숙 | 비추천 |

#### Frontend 프레임워크

| 선택지 | 장점 | 단점 | 추천 |
|---|---|---|---|
| **React + TypeScript** | 생태계, 컴포넌트 풍부 | 보일러플레이트 | **추천** |
| Vue 3 + TypeScript | 학습 곡선 낮음, Composition API | 관리 도구 전용 컴포넌트 상대적 부족 | 대안 |
| Svelte | 번들 크기 최소 | 생태계 규모 작음 | 비추천 |
