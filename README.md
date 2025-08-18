# 자동 평가 파이프라인 오케스트레이터

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/release/python-390/)
[![managed with Poetry](https://img.shields.io/badge/managed%20with-Poetry-blueviolet.svg)](https://python-poetry.org/)
[![Project Status: Active](https://img.shields.io/badge/project%20status-active-brightgreen.svg)](https://www.repostatus.org/#active)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Orchestrator v5.0 - Poetry Edition

이 프로젝트는 여러 독립적인 평가 도구들을 체계적으로 연결하여 학생들의 코딩 제출물을 자동으로 평가하는 강력한 **Poetry 기반** 오케스트레이션 애플리케이션입니다.

단일 명령 실행만으로 **(1)사전 분석**, **(2)개별 병렬 처리(복호화, 코드 복원, 일치도 검사, 과정 분석)**, **(3)최종 리포팅**의 복잡한 파이프라인을 완벽하게 자동화합니다.

---

## 🚀 주요 기능

-   **Poetry 기반 프로젝트**: `pyproject.toml`을 통해 모든 의존성을 명확하고 재현 가능하게 관리합니다. `poetry install` 한 번으로 완벽한 실행 환경이 구성됩니다.
-   **완전 자동화**: 단일 명령으로 전체 평가 파이프라인을 실행하며, 필요한 Poetry 기반 도구의 의존성(`mission-decoder` 등)도 자동으로 준비합니다.
-   **고성능 병렬 처리**: 다수의 학생 제출물을 시스템의 모든 CPU 코어를 활용하여 동시에 처리함으로써, 평가 시간을 획기적으로 단축시킵니다.
-   **설정 파일 기반 유연성**: `config.json`을 통해 프로젝트의 모든 경로와 파일 구조를 외부에서 손쉽게 제어할 수 있어, 코드 변경 없이 다양한 환경에 적용 가능합니다.
-   **다단계 파이프라인**: 각 학생 제출물에 대해 다음의 4단계 분석을 순차적으로 수행합니다.
    1.  **복호화**: `mission-decoder`를 사용하여 암호화된 로그와 서명 파일을 복호화합니다.
    2.  **코드 복원**: `mission-restore`를 사용하여 복호화된 로그로부터 원본 소스 코드를 복원합니다.
    3.  **일치도 검사**: `loose-diff`를 사용하여 제출된 원본 코드와 복원된 코드의 실질적 동일성을 검증합니다.
    4.  **과정 분석**: `mission-inspector (a.k.a inspector)`를 사용하여 코드 개발 과정의 품질을 정량적인 점수로 평가합니다.
-   **사전 중복 분석**: 전체 제출물을 대상으로 `duplicate-finder`를 실행하여 의심스러운 중복 로그 파일을 사전에 탐지하고 리포트에 그룹으로 표기합니다.
-   **상세 리포팅 및 로깅**:
    -   **CSV 리포트**: 모든 분석 결과를 종합하여 `evaluation_report.csv` 파일로 깔끔하게 정리합니다.
    -   **상세 로그**: 모든 실행 과정과 오류를 `orchestrator.log` 파일에 타임스탬프와 함께 기록하여 추적 및 디버깅이 용이합니다.
-   **직관적인 사용자 경험**: `tqdm` 진행률 표시줄을 통해 전체 작업 현황, 처리 속도, 남은 시간을 실시간으로 확인할 수 있습니다.

---

## 🛠️ 실행 전 필수 환경 준비

이 오케스트레이터를 성공적으로 실행하려면, 다음과 같은 폴더 구조와 파일, 그리고 시스템 요구사항이 정확하게 준비되어 있어야 합니다.

### 1. 전체 폴더 구조

스크립트는 `config.json`에 정의된 구조를 기준으로 동작합니다. 기본 설정은 아래와 같습니다. **모든 명령어는 `work` 폴더에서 실행**하는 것을 기준으로 합니다.

```
project-root/
├── student_submission/           # (필수) 평가 대상 학생들의 프로젝트 폴더
│   ├── student-01/
│   │   └── src/mission_python/main.py       
│   │   └── src/mission_python/log/log.encrypted
│   │   └── src/mission_python/log/signature.encrypted
│   └── ...
│
├── tools/                        # (필수) 모든 분석 도구들이 위치하는 폴더
│   ├── bin/                      # (권장) 컴파일된 실행 파일들을 모아두는 곳
│   │   ├── duplicate_finder
│   │   └── inspector
│   ├── loose-diff/ldiff.py
│   ├── mission-decoder/
│   ├── mission-restore/
│   └── mission-decoder.keys/
│       └── private_key.pem
│
├── work/                         # (필수) 이 오케스트레이터 프로젝트 폴더
│   ├── orchestrator/
│   ├── config.json
│   ├── pyproject.toml
│   ├── poetry.lock
│   └── output/                   # 실행 시 자동 생성
│
├── HTML-REPORT-SAMPLES/          # (참고) 최종 생성될 HTML 리포트 샘플
└── MISSION-INSPECTOR-OVERALL/    # (참고) Mission Inspector 설명 논문
```

### 2. 시스템 요구사항

-   **Python 3.9 이상**
-   **Poetry**: 시스템 `PATH`에 `poetry` 명령어가 등록되어 있어야 합니다.
    -   설치 확인: 터미널에서 `poetry --version` 실행

### 3. 외부 도구 준비 상태

-   **Poetry 기반 도구 (`mission-decoder`, `mission-restore`):** 폴더 내에 `pyproject.toml` 파일이 존재해야 합니다. 오케스트레이터가 `poetry install`을 자동으로 실행하므로 별도의 준비는 불필요합니다.
-   **컴파일된 도구 (`duplicate-finder`, `inspector`):** `config.json`에 명시된 경로(기본값: `tools/bin/`)에 **미리 컴파일된 실행 파일**이 존재해야 합니다.

### 4. 설정 파일 (`config.json`)

이 프로젝트의 모든 동작은 `work/config.json` 파일을 통해 제어됩니다. 폴더 구조가 변경되거나 사용하는 파일 이름이 바뀔 경우, **코드 수정 없이 이 파일만 변경하면 됩니다.**

```json
{
  "directories": { // ...
  },
  "tools": { // ...
  },
  "student_file_structure": { // ...
  },
  "output_files": { // ...
  }
}
```

---

## ⚙️ 사용 방법

### 1. 초기 설정 (최초 1회만 실행)

1.  **터미널을 열고 `work` 폴더로 이동합니다.**
    ```bash
    cd /path/to/project-root/work
    ```

2.  **Poetry를 사용하여 의존성을 설치합니다.**
    ```bash
    poetry install
    ```

### 2. 파이프라인 실행

1.  **`work` 폴더에서 아래 명령어를 실행합니다.**
    `--duration` 인자는 **필수**이며, `inspector`가 개발 과정 분석 시 참고할 총 시험 시간을 분 단위로 지정합니다.

    ```bash
    # 기본 실행 (시험 시간이 30분인 경우)
    poetry run orchestrate --duration 30
    ```
    *(`orchestrate`는 `pyproject.toml`에 `tool.poetry.scripts`로 정의된 편리한 실행 명령어입니다.)*

### 3. 실행 결과물 확인

-   **콘솔 출력**: 전체 진행 상황이 `tqdm` 진행률 표시줄을 통해 시각적으로 표시됩니다.

    ```
    Processing students: 100%|████████████| 15/15 [00:45<00:00,  3.00s/it]
    ```

-   **최종 리포트**: `work/output/report/evaluation_report.csv`에 모든 학생의 최종 평가 결과가 종합되어 저장됩니다.

    | student_id | status | duplication\_group | process\_analysis\_score | location |
    | :--- | :--- | :--- | :--- | :--- |
    | student-01 | OK | UNIQUE | 95 | Seoul |
    | ... | ... | ... | ... | ... |

-   **학생별 산출물**: `work/output/processed_outputs/` 폴더 아래에 각 학생 ID로 된 폴더가 생성되며, 복호화된 로그, 복원된 코드, 과정 분석 HTML 리포트 등 모든 중간/최종 산출물이 저장됩니다. 생성되는 HTML 리포트의 예시는 프로젝트 루트의 `HTML-REPORT-SAMPLES` 폴더에서 미리 확인해볼 수 있습니다.

-   **상세 로그**: 파이프라인의 모든 세부 실행 과정, 경고, 오류는 `work/orchestrator.log`에 기록됩니다. 문제 발생 시 가장 먼저 확인해야 할 파일입니다.

---

## ⚠️ 중요: 외부 도구 출력 형식 의존성

이 오케스트레이터는 일부 외부 도구의 **표준 출력(stdout) 텍스트를 파싱**하여 로직을 처리합니다. 따라서 해당 도구들의 화면 출력 형식이 변경될 경우, **오케스트레이터가 오작동하거나 실패할 수 있습니다.** 유지보수 시 아래의 의존성을 반드시 인지하고 주의해야 합니다.

| 사용하는 도구 | 파싱하는 화면 출력 내용 | 오케스트레이터의 역할 |
| :--- | :--- | :--- |
| **`loose-diff`** | `"✅ 파일이 실질적으로 동일합니다"` | 원본과 복원된 코드의 일치 여부를 판단 (`OK` vs. `DIFFERENT`) |
| **`duplicate-finder`**| `"--- 그룹 ..."` 와 `" - ..."` 라인의 경로 텍스트| 중복된 로그 파일을 제출한 학생들을 그룹으로 묶고 ID 부여 |
| **`inspector`** | `"⏺︎ 최종 앙상블 점수: (\d+) / 100"` | 정규식을 사용하여 학생의 최종 개발 과정 점수를 추출 |

만약 위 도구들의 업데이트로 인해 출력 텍스트가 변경된다면, 오케스트레이터의 해당 파싱 로직(`orchestrator/main.py` 내부)도 함께 수정해야 합니다.

---

## 🔗 관련 프로젝트 및 외부 도구

이 오케스트레이션 파이프라인에서 사용하는 주요 외부 도구 및 프로젝트들은 다음과 같습니다:

-   **[mission-python](https://github.com/drsungwon/mission-python)**: 평가 대상이 되는 학생 코딩 미션의 기본 구조입니다.
-   **[mission-decoder](https://github.com/drsungwon/mission-decoder)**: 암호화된 로그와 서명 파일을 복호화하는 도구입니다.
-   **[mission-restore](https://github.com/drsungwon/mission-restore)**: 복호화된 로그로부터 원본 소스 코드를 복원하는 도구입니다.
-   **[loose-diff](https://github.com/drsungwon/loose-diff)**: 복원된 코드와 원본 코드를 실질적으로 비교하는 도구입니다.
-   **[duplicate-finder](https://github.com/drsungwon/duplicate_finder)**: 제출된 로그 파일 간의 중복을 탐지하는 도구입니다.
-   **[hybrid-encryption](https://github.com/drsungwon/hybrid_encryption)**: (참고: `mission-decoder` 등에서 사용되는 암호화 모듈)
-   **MISSION-INSPECTOR-OVERALL**: `Mission Inspector`가 학생들의 개발 과정을 어떻게 정량적으로 평가하는지에 대한 상세 설명이 담긴 논문이 포함되어 있습니다.