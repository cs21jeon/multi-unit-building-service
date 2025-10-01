# Multi-Unit Building Service

집합건물(아파트, 오피스텔 등)의 개별 세대/호실 정보를 자동으로 수집하고 관리하는 서비스입니다.

## 🏢 주요 기능

### 데이터 수집
- **전용면적**: 주건축물의 전유 부분 면적 자동 계산
- **공용면적**: 주건축물의 공용 부분 면적 자동 계산  
- **공급면적**: 전용면적 + 공용면적 자동 계산
- **건축 정보**: 대지면적, 건축면적, 연면적, 용적률, 건폐율
- **건물 정보**: 높이, 층수, 주구조, 지붕, 주용도, 기타용도
- **부대시설**: 승강기수, 주차대수, 총 세대수
- **법적 정보**: 사용승인일, 용도지역

### 처리 대상
- 아파트 개별 세대
- 오피스텔 개별 호실
- 연립주택 개별 호실
- 다세대주택 개별 호실
- 상가 개별 호실
- 기타 집합건물

## 🚀 설치 및 실행

### 1. 의존성 설치
```bash
npm install
```

### 2. 환경 변수 설정
`.env` 파일에 다음 환경 변수들을 설정하세요:

```env
# Airtable 설정 (집합건물 전용)
AIRTABLE_ACCESS_TOKEN=****
# 집합건물 베이스 ID: appQkFdB8TdPVNWdz
# 집합건물 테이블 ID: tblT28nHoneqlbgBh  
# 집합건물 뷰 ID: viwOs5jlYkIGPZ142

# 공공데이터포털 API (기존 building-service와 공유)
PUBLIC_API_KEY=your_public_api_key

# Google Scripts (주소-코드 변환용, 기존과 공유)
GOOGLE_SCRIPT_URL=your_google_script_url

# 서비스 설정
MULTI_UNIT_BUILDING_SERVICE_PORT=3002
LOG_LEVEL=info
```

### 3. 서비스 실행

#### 개발 모드
```bash
npm run dev
```

#### 프로덕션 모드
```bash
npm start
```

#### PM2로 실행
```bash
pm2 start multi-unit-app.js --name multi-unit-building-service
```

### 4. 자동 배포
```bash
sudo ./deploy-script.sh
```

## 📊 Airtable 데이터 구조

### 입력 필드 (필수)
- **지번 주소**: 예) "동작구 사당동 1157"
- **동**: 예) "102동", "A동"
- **호수**: 예) "1003", "301호"

### 자동 입력 필드
- **도로명주소**: 공공데이터에서 자동 입력
- **건물명**: 공공데이터에서 자동 입력
- **공급면적(㎡)**: 전용면적 + 공용면적
- **전용면적(㎡)**: 전유 부분 면적의 합
- **대지지분(㎡)**: 개별 세대의 대지 지분
- **대지면적(㎡)**: 전체 대지면적
- **연면적(㎡)**: 전체 연면적
- **용적률산정용연면적(㎡)**: 용적률 계산용 연면적
- **건축면적(㎡)**: 전체 건축면적
- **건폐율(%)**: 건폐율
- **용적률(%)**: 용적률
- **높이(m)**: 건물 높이
- **주구조**: 주요 구조 (예: 철근콘크리트구조)
- **지붕**: 지붕 재료
- **주용도**: 주요 용도
- **총층수**: 지하층/지상층 형태 (예: -3/15)
- **총 세대/가구/호**: 전체 세대수 정보
- **해당동 세대/가구/호**: 해당 동의 세대수 정보
- **총주차대수**: 전체 주차 대수
- **승강기수**: 승용 + 비상용 승강기 수
- **사용승인일**: 건물 사용승인 날짜
- **용도지역**: 도시계획상 용도지역

## 🔧 API 엔드포인트

### GET /health
서비스 상태 확인
```json
{
  "status": "ok",
  "service": "multi-unit-building-service",
  "timestamp": "2025-06-23T10:30:00.000Z",
  "version": "1.0.0"
}
```

### GET /run-job
집합건물 정보 수집 작업 수동 실행
```json
{
  "message": "Multi-unit building job completed",
  "result": {
    "total": 10,
    "success": 8
  }
}
```

### GET /
웹 관리 인터페이스 접속

## 📋 데이터 처리 로직

### 면적 계산 로직
```javascript
// 전용면적: mainAtchGbCdNm이 "주건축물"이고 exposPubuseGbCdNm이 "전유"인 경우
전용면적 = sum(area where mainAtchGbCdNm="주건축물" AND exposPubuseGbCdNm="전유")

// 공용면적: mainAtchGbCdNm이 "주건축물"이고 exposPubuseGbCdNm이 "공용"인 경우  
공용면적 = sum(area where mainAtchGbCdNm="주건축물" AND exposPubuseGbCdNm="공용")

// 공급면적: 전용면적 + 공용면적
공급면적 = 전용면적 + 공용면적
```

### API 호출 순서
1. **주소 파싱**: 지번 주소를 시군구, 법정동, 번, 지로 분리
2. **코드 조회**: Google Scripts를 통해 시군구코드, 법정동코드 획득
3. **건축물 정보 조회**:
   - `getBrTitleInfo`: 건축물 표제부 정보
   - `getBrRecapTitleInfo`: 총괄표제부 정보  
   - `getBrExposPubuseAreaInfo`: 전유공용면적 정보 (동/호수별)
   - `getBrJijiguInfo`: 지구지역 정보
4. **데이터 가공**: 각 API 응답을 통합하여 최종 데이터 생성
5. **Airtable 업데이트**: 가공된 데이터를 Airtable에 저장

## ⚙️ 설정 파일

### Nginx 설정 예시
```nginx
server {
    listen 80;
    server_name multi-unit-building.goldenrabbit.biz;

    location / {
        proxy_pass http://localhost:3002;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### PM2 Ecosystem 설정 (ecosystem.config.js)
```javascript
module.exports = {
  apps: [{
    name: 'multi-unit-building-service',
    script: 'multi-unit-app.js',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production',
      PORT: 3002
    }
  }]
}
```

## 📝 로그 관리

### 로그 레벨
- `debug`: 상세한 디버깅 정보
- `info`: 일반적인 정보 메시지
- `warn`: 경고 메시지
- `error`: 오류 메시지

### 로그 파일 위치
```
/home/multi-unit-building-service/logs/
├── 2025-06-23.log
├── 2025-06-24.log
└── ...
```

### 로그 확인 명령어
```bash
# 실시간 로그 확인
pm2 logs multi-unit-building-service

# 특정 날짜 로그 확인
tail -f logs/2025-06-23.log

# 오류 로그만 확인
grep "ERROR" logs/2025-06-23.log
```

## 🔄 자동화 스케줄링

서비스는 매분마다 Airtable을 확인하여 처리할 레코드가 있는지 검사합니다.

```javascript
// 매분 실행
cron.schedule('* * * * *', async () => {
  // 처리할 레코드 확인
  // 있으면 자동 처리 실행
});
```

## 🚨 오류 처리

### 일반적인 오류 및 해결방법

1. **주소 파싱 오류**
   - 원인: 잘못된 주소 형식
   - 해결: "구/시/군 법정동 번-지" 형식으로 입력

2. **API 키 오류**
   - 원인: 공공데이터포털 API 키 문제
   - 해결: `.env` 파일의 `PUBLIC_API_KEY` 확인

3. **Airtable 연결 오류**
   - 원인: Airtable 접근 토큰 또는 베이스 ID 문제
   - 해결: `.env` 파일의 Airtable 설정 확인

4. **데이터 없음**
   - 원인: 해당 주소의 건축물 정보가 공공데이터에 없음
   - 해결: 주소 확인 또는 수동 입력

## 📊 모니터링

### 서비스 상태 확인
```bash
# PM2 상태 확인
pm2 status multi-unit-building-service

# 서버 리소스 확인
pm2 monit

# 서비스 재시작
pm2 restart multi-unit-building-service
```

### 성능 지표
- 처리 성공률
- 평균 처리 시간
- API 응답 시간
- 메모리 사용량

## 🔧 개발자 정보

### 프로젝트 구조
```
multi-unit-building-service/
├── multi-unit-app.js      # 메인 애플리케이션 (이름 변경됨)
├── package.json           # 의존성 정보
├── deploy-script.sh       # 배포 스크립트
├── README.md             # 문서
├── .env                  # 환경 변수 (gitignore)
├── public/
│   └── index.html        # 웹 인터페이스
└── logs/                 # 로그 파일 디렉토리
    └── YYYY-MM-DD.log
```

### 주요 의존성
- `express`: 웹 서버 프레임워크
- `airtable`: Airtable API 클라이언트
- `axios`: HTTP 클라이언트
- `node-cron`: 스케줄 작업
- `dotenv`: 환경 변수 관리

## 📞 지원

### 기술 지원
- 로그 파일 확인: `/home/multi-unit-building-service/logs/`
- PM2 로그: `pm2 logs multi-unit-building-service`
- 상태 확인: `http://your-domain/health`

### 버전 정보
- 현재 버전: 1.0.0
- 최소 Node.js 버전: 16.0.0
- 지원 OS: Linux (Ubuntu 20.04+)

---

© 2025 GoldenRabbit. All rights reserved.
