# chzzk-load

[English](README.md) | **한국어**

[![npm version](https://img.shields.io/npm/v/chzzk-load.svg?logo=npm)](https://www.npmjs.com/package/chzzk-load)
[![GitHub Release](https://img.shields.io/github/v/release/ghfhffh12345/chzzk-load?logo=github)](https://github.com/ghfhffh12345/chzzk-load/releases)
[![CI](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml/badge.svg)](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

네이버 치지직(Chzzk) 라이브 방송을 자동으로 감지하여 실시간 영상 및 라이브 채팅을 녹화하고 Google Drive로 동기화하는 고성능 독립 실행형 CLI 도구입니다. [Ratatui](https://github.com/ratatui/ratatui) 기반의 대화형 터미널 사용자 인터페이스(TUI)를 제공합니다.

`chzzk-load`는 방송 상태를 실시간 모니터링하며, FFmpeg 스트림 복사(`-c copy`)를 통해 원본 손실 없이 영상을 MPEG-TS 세그먼트로 분할 저장하고 WebSocket을 통해 실시간 라이브 채팅을 구조화된 JSON Lines(`chat.jsonl`) 형식으로 동시 수집합니다. 분할 완료된 영상 세그먼트와 채팅 로그는 백그라운드에서 Google Drive로 즉시 업로드되며, 업로드 성공이 확인되는 즉시 로컬 파일을 삭제하여 디스크 사용량을 최소한으로 엄격히 유지합니다.

![chzzk-load TUI 대시보드](assets/tui-preview.png)

---

## 주요 기능

- ⚡ **무손실 스트림 복사 및 깔끔한 정상 종료 (`-c copy`)**: 라이브 HLS 비디오 스트림을 재인코딩 없이 원본 그대로 `.ts` 조각으로 분할하여 CPU 및 메모리 부하를 최소화합니다. 재연결 플래그를 생략하여 방송 종료 시 HLS 매니페스트 EOF 무한 재시도 루프를 원천 차단합니다.
- 🌐 **직접 CDN 스트림 주소 자동 추출 (P2P/그리드 우회)**: 치지직 `p2pPath` 플레이리스트에 포함된 base64 인코딩 `cdn_url` 매개변수를 자동 디코딩하여, 별도의 그리드 소프트웨어 설치 없이 원본 화질(1080p, 720p 등)의 직접 CDN HLS 스트림을 수집합니다.
- 💬 **실시간 라이브 채팅 녹화 (`chat.jsonl`)**: WebSocket을 통해 실시간 방송 채팅을 동시 수집하여 타임스탬프, 사용자 닉네임, 뱃지, 후원(치즈) 내역, 메시지 내용이 포함된 구조화된 JSON Lines 형식으로 저장합니다.
- 💽 **플래시 수명 보호 배치 I/O (SBC 최적화)**: 라즈베리 파이(Raspberry Pi), ARM64 등 단일 보드 컴퓨터(SBC)의 microSD 및 플래시 메모리 수명을 보존하기 위해 바이트 버퍼 메모리 버퍼링 및 듀얼 트리거 플러시(500개 메시지 / 64KB 도달 또는 주기적 타이머)를 적용하여 디스크 쓰기 빈도를 최소화합니다.
- 🏷️ **실시간 방송 제목 추적 및 폴더 동기화**: 방송 중 변경되는 방제를 자동으로 감지하여 `title_history.txt`에 기록하고, Google Drive의 폴더 이름을 최신 방제로 실시간 자동 갱신합니다.
- 💾 **엄격히 제한된 디스크 사용량**: 활성 스트림당 최대 1~2개의 세그먼트 파일만 로컬 디스크에 유지합니다. 클라우드 업로드 완료가 확인되는 즉시 영상 세그먼트와 채팅 로그 파일은 로컬에서 영구 삭제됩니다.
- 🛡️ **N+1 세그먼트 경계 안전성**: $N$번째 청크는 다음 $N+1$번째 청크가 디스크에 생성(파일 크기 > 0)된 것이 확인된 후에만 업로드 큐로 전달되어, 불완전하거나 손상된 청크의 업로드를 원천 차단합니다.
- ☁️ **안정적인 Google Drive 이어올리기 및 로컬 폴백**: Google Drive API v3 및 자동 PKCE OAuth2 인증을 통한 클라우드 실시간 전송을 지원합니다. 루트 폴더 캐싱 및 지수 백오프 재시도(HTTP 429 및 5xx 대응)를 갖추고 있으며, Google Drive 인증 설정이 없으면 자동으로 로컬 단독 녹화 모드로 동작합니다.
- 🔀 **채널별 순차 직렬화 & 다중 스트림 동시 업로드**: 동일 채널의 세그먼트는 순차(FIFO) 업로드를 보장하여 순서 꼬임과 대역폭 경합을 방지하며, 여러 채널 간에는 최대 `upload_concurrency`(기본값: 3)개까지 병렬 업로드합니다.
- 🖥️ **이벤트 기반 터미널 대시보드 (TUI)**: `crossterm::event::EventStream` 기반의 비동기 이벤트 루프와 제로 메모리 할당 렌더링으로 유휴 CPU 점유율을 0으로 억제하며, 실시간 채널 상태, 방송 제목, 실시간 수집 채팅 수 카운터, 업로드 진행률 게이지, 전송 속도 지표, 헤더 요약 통계(활성 녹화 수, 누적 녹화 시간, 아카이브 용량), 로그 토글 기능(`l` 키), Windows 콘솔 UTF-8 코드페이지 자동 설정을 지원합니다.
- 🔄 **CDN 캐시 지연 중복 방지 (Anti-Race)**: 방송 종료 후 쿨다운 적용 및 방송 고유 세션 ID(`live_id`) 추적을 통해 치지직 CDN 캐시 지연(10~30초)으로 인한 중복 세션 생성을 방지합니다.

---

## 사전 요구사항

- **FFmpeg**: 시스템의 `PATH` 환경 변수에 등록되어 있어야 합니다 (또는 `CHZZK_LOAD_FFMPEG_BIN` 환경 변수로 실행 파일 경로 지정 가능).

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
    "min_free_disk_gb": 2.0,
    "record_chat": true,
    "chat_flush_interval_seconds": 30
  },
  "google_drive": {
    "credentials_path": "credentials.json",
    "token_path": "token.json",
    "root_folder_name": "Chzzk_Recordings",
    "upload_concurrency": 3
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
| `general.recordings_dir` | `"recordings"` | 임시 세그먼트 영상 파일 및 채팅 로그가 저장되는 로컬 디렉터리 경로. |
| `general.min_free_disk_gb` | `2.0` | 녹화를 계속하기 위해 필요한 최소 여유 디스크 공간(GB 단위). |
| `general.record_chat` | `true` | `chat.jsonl` 파일로 실시간 라이브 채팅 동시 녹화 활성화 여부. |
| `general.chat_flush_interval_seconds` | `30` | 메모리에 버퍼링된 채팅 메시지를 디스크로 플러시하는 주기(초 단위). |
| `google_drive.credentials_path` | `"credentials.json"` | Google Cloud에서 발급받은 OAuth2 데스크톱 클라이언트 비밀번호 파일 경로. |
| `google_drive.token_path` | `"token.json"` | 발급받은 OAuth2 인증 토큰이 자동 저장 및 갱신되는 파일 경로. |
| `google_drive.root_folder_name` | `"Chzzk_Recordings"` | Google Drive 내에 녹화 파일들이 업로드될 루트 폴더 이름. |
| `google_drive.upload_concurrency` | `3` | 채널 간 동시 업로드 가능한 최대 스트림 수 (동일 채널 내 청크는 엄격한 FIFO 순서로 직렬 업로드됨). |
| `chzzk.nid_aut` / `nid_ses` | `""` | 연령 제한 또는 구독자 전용 방송 녹화를 위한 네이버 로그인 세션 쿠키 값 (선택 사항). |
| `channels` | - | 모니터링할 치지직 채널 목록 (`id`: 채널 URL의 고유 식별자, `name`: TUI 표시용 이름). |

---

## 환경 변수 안내

| 환경 변수 | 설명 |
| :--- | :--- |
| `CHZZK_LOAD_FFMPEG_BIN` | FFmpeg 실행 파일의 사용자 지정 경로 (미설정 시 기본적으로 `PATH`의 `ffmpeg` 사용). |
| `CHZZK_LOAD_BIN` | npm 런처 사용 시 실행할 네이티브 `chzzk-load` 바이너리의 사용자 지정 경로. |

---

## Google Drive 연동 설정

Google Drive 인증 정보가 설정되지 않은 경우, `chzzk-load`는 자동으로 **로컬 전용 녹화 모드**로 전환되어 `recordings_dir`에 `.ts` 파일들과 `chat.jsonl` 파일을 보관합니다.

Google Drive 자동 업로드를 활성화하려면:
1. [Google Cloud Console](https://console.cloud.google.com/)에서 프로젝트를 생성하고 **Google Drive API**를 활성화합니다.
2. **사용자 인증 정보** $\to$ **사용자 인증 정보 만들기** $\to$ **OAuth 클라이언트 ID**에서 애플리케이션 유형으로 **데스크톱 앱(Desktop App)**을 선택합니다.
3. 생성된 클라이언트 비밀번호 JSON 파일을 다운로드하여 `credentials.json`으로 이름을 변경한 후, `settings.json`과 동일한 디렉터리에 배치합니다.
4. `chzzk-load`를 실행합니다. 일회성 OAuth2 인증을 위한 브라우저 창이 자동으로 열립니다. 승인 완료 시 인증 토큰이 `token.json`에 자동 저장되며 이후 실행 시 자동으로 갱신됩니다.

---

## 단축키 안내

| 키 | 동작 |
| :--- | :--- |
| `q` | **종료 (Quit)**: 안전한 정상 종료 절차를 시작합니다 (진행 중인 녹화 프로세스를 정상 중단하고, 채팅 버퍼를 플러시하며, 대기 중인 업로드를 마무리). 한 번 더 `q` 또는 `Ctrl+C`를 누르면 즉시 강제 종료됩니다. |
| `l` | **로그 토글 (Toggle Logs)**: 활동 로그 섹션을 표시하거나 숨깁니다 (로그를 숨기면 채널 및 클라우드 업로드 영역이 확장됩니다). |
| `r` | **새로고침 (Refresh)**: 등록된 채널들의 방송 상태를 즉시 다시 확인합니다. |
| `↑` / `k` | **위로 스크롤**: 모니터링 채널 및 클라우드 업로드 목록을 위로 스크롤합니다. |
| `↓` / `j` | **아래로 스크롤**: 모니터링 채널 및 클라우드 업로드 목록을 아래로 스크롤합니다. |
| `PageUp` / `PageDown` | **로그 스크롤**: 활동 로그를 5줄 단위로 위/아래 스크롤합니다. |
| `Home` / `End` | **로그 이동**: 맨 위(가장 오래된 로그)로 이동하거나 맨 아래(최신 로그, 자동 스크롤 재개)로 이동합니다. |

---

## 라이선스

Apache License 2.0에 따라 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.
