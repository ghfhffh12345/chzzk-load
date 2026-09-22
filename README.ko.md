# chzzk-load

[English](README.md) | **한국어**

[![npm version](https://img.shields.io/npm/v/chzzk-load.svg?logo=npm)](https://www.npmjs.com/package/chzzk-load)
[![GitHub Release](https://img.shields.io/github/v/release/ghfhffh12345/chzzk-load?logo=github)](https://github.com/ghfhffh12345/chzzk-load/releases)
[![CI](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml/badge.svg)](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

네이버 치지직(Chzzk) 라이브 방송을 자동으로 감지하여 실시간으로 녹화하고 Google Drive로 동기화하는 고성능 독립 실행형 CLI 도구입니다. [Ratatui](https://github.com/ratatui/ratatui) 기반의 대화형 터미널 사용자 인터페이스(TUI)를 제공합니다.

`chzzk-load`는 방송 상태를 실시간 모니터링하며, FFmpeg 스트림 복사(`-c copy`)를 통해 원본 손실 없이 영상을 MPEG-TS 세그먼트로 분할 저장합니다. 분할 완료된 세그먼트는 백그라운드에서 Google Drive로 즉시 업로드되며, 업로드 성공이 확인되는 즉시 로컬 파일을 삭제하여 디스크 사용량을 최소한으로 엄격히 유지합니다.

![chzzk-load TUI 대시보드](assets/tui-preview.png)

---

## 주요 기능

- ⚡ **무손실 스트림 복사 (`-c copy`)**: 라이브 HLS 비디오 스트림을 재인코딩 없이 원본 그대로 `.ts` 조각으로 분할하여 CPU 및 메모리 부하를 최소화합니다.
- 💾 **엄격히 제한된 디스크 사용량**: 활성 스트림당 최대 1~2개의 세그먼트 파일만 로컬 디스크에 유지합니다. 클라우드 업로드 완료가 확인되는 즉시 로컬 파일은 영구 삭제됩니다.
- 🛡️ **N+1 세그먼트 경계 안전성**: $N$번째 청크는 다음 $N+1$번째 청크가 디스크에 생성(파일 크기 > 0)된 것이 확인된 후에만 업로드 큐로 전달되어, 불완전하거나 손상된 청크의 업로드를 원천 차단합니다.
- ☁️ **Google Drive 이어올리기(Resumable) 및 로컬 폴백**: Google Drive API v3 및 자동 PKCE OAuth2 인증을 통한 클라우드 실시간 전송을 지원합니다. Google Drive 인증 설정이 없으면 자동으로 로컬 단독 녹화 모드로 동작합니다.
- 🖥️ **대화형 터미널 대시보드 (TUI)**: 실시간 채널 상태, 방송 제목, 업로드 진행률 게이지, 전송 속도, 확보된 디스크 용량, 실시간 로그 등을 한 화면에서 모니터링할 수 있습니다.
- 🔄 **CDN 캐시 지연 중복 방지 (Anti-Race)**: 방송 종료 후 쿨다운 적용 및 방송 고유 세션 ID(`live_id`) 추적을 통해 치지직 CDN 캐시 지연(10~30초)으로 인한 중복 세션 생성을 방지합니다.

---

## 사전 요구사항

- **FFmpeg**: 시스템의 `PATH` 환경 변수에 등록되어 있어야 합니다.

```bash
ffmpeg -version
```

---

## 설치 및 빠른 시작

npm을 통해 글로벌로 설치합니다:

```bash
npm install -g chzzk-load
```

애플리케이션 실행:

```bash
# 기본 설정으로 실행 (설정 파일이 없으면 자동으로 settings.json 템플릿 생성)
chzzk-load

# 또는 사용자 지정 설정 파일 경로 지정
chzzk-load --config /path/to/my-settings.json
```

최초 실행 시 현재 작업 디렉터리에 `settings.json` 파일이 존재하지 않는 경우 기본 템플릿이 자동으로 생성됩니다.

---

## 설정 가이드 (`settings.json`)

```json
{
  "general": {
    "chunk_duration_seconds": 600,
    "poll_interval_seconds": 20,
    "stream_cooldown_seconds": 60,
    "recordings_dir": "recordings",
    "min_free_disk_gb": 2.0
  },
  "google_drive": {
    "credentials_path": "credentials.json",
    "token_path": "token.json",
    "root_folder_name": "Chzzk_Recordings"
  },
  "chzzk": {
    "nid_aut": "",
    "nid_ses": ""
  },
  "channels": [
    {
      "id": "1a1dd9ce56fb61a37ffb6f69f6d5b978",
      "name": "강퀴"
    }
  ]
}
```

### 주요 설정 항목

| 항목 | 기본값 | 설명 |
| :--- | :--- | :--- |
| `general.chunk_duration_seconds` | `600` (10분) | 분할 녹화할 영상 세그먼트의 길이(초 단위). |
| `general.poll_interval_seconds` | `20` | 치지직 라이브 방송 시작 여부를 확인하는 폴링 주기(초 단위). |
| `general.stream_cooldown_seconds` | `60` | 방송 종료 후 CDN 캐시 잔여로 인한 중복 녹화를 방지하기 위한 대기 시간(초 단위). |
| `general.recordings_dir` | `"recordings"` | 임시 세그먼트 영상 파일이 저장되는 로컬 디렉터리 경로. |
| `general.min_free_disk_gb` | `2.0` | 녹화를 계속하기 위해 필요한 최소 여유 디스크 공간(GB 단위). |
| `google_drive.credentials_path` | `"credentials.json"` | Google Cloud에서 발급받은 OAuth2 데스크톱 클라이언트 비밀번호 파일 경로. |
| `google_drive.root_folder_name` | `"Chzzk_Recordings"` | Google Drive 내에 녹화 파일들이 업로드될 루트 폴더 이름. |
| `chzzk.nid_aut` / `nid_ses` | `""` | 연령 제한 또는 구독자 전용 방송 녹화를 위한 네이버 로그인 세션 쿠키 값 (선택 사항). |
| `channels` | - | 모니터링할 치지직 채널 목록 (`id`: 채널 URL의 고유 식별자, `name`: TUI 표시용 이름). |

---

## Google Drive 연동 설정

Google Drive 인증 정보가 설정되지 않은 경우, `chzzk-load`는 자동으로 **로컬 전용 녹화 모드**로 전환되어 `recordings_dir`에 `.ts` 파일들을 보관합니다.

Google Drive 자동 업로드를 활성화하려면:
1. [Google Cloud Console](https://console.cloud.google.com/)에서 프로젝트를 생성하고 **Google Drive API**를 활성화합니다.
2. **사용자 인증 정보** $\to$ **사용자 인증 정보 만들기** $\to$ **OAuth 클라이언트 ID**에서 애플리케이션 유형으로 **데스크톱 앱(Desktop App)**을 선택합니다.
3. 생성된 클라이언트 비밀번호 JSON 파일을 다운로드하여 `credentials.json`으로 이름을 변경한 후, `settings.json`과 동일한 디렉터리에 배치합니다.
4. `chzzk-load`를 실행합니다. 일회성 OAuth2 인증을 위한 브라우저 창이 자동으로 열립니다. 승인 완료 시 인증 토큰이 `token.json`에 자동 저장되며 이후 실행 시 자동으로 갱신됩니다.

---

## 단축키 안내

| 키 | 동작 |
| :--- | :--- |
| `q` | **종료 (Quit)**: 안전한 정상 종료 절차를 시작합니다 (진행 중인 녹화 프로세스를 정상 중단하고 대기 중인 업로드를 마무리). |
| `r` | **새로고침 (Refresh)**: 등록된 채널들의 방송 상태를 즉시 다시 확인합니다. |
| `↑` / `k` | **위로 이동**: 채널 목록에서 이전 채널을 선택합니다. |
| `↓` / `j` | **아래로 이동**: 채널 목록에서 다음 채널을 선택합니다. |
| `PageUp` / `PageDown` | **로그 스크롤**: 활동 로그를 5줄 단위로 위/아래 스크롤합니다. |
| `Home` / `End` | **로그 이동**: 맨 위(가장 오래된 로그)로 이동하거나 맨 아래(최신 로그, 자동 스크롤 재개)로 이동합니다. |

---

## 라이선스

Apache License 2.0에 따라 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.
