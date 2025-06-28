#!/bin/bash
# 집합건물 세대별 정보 서비스 배포 스크립트
# 사용법: ./deploy-script.sh

# 설정 변수
SERVICE_NAME="multi-unit-building-service"
TARGET_DIR="/home/multi-unit-building-service"
NGINX_CONFIG_SRC="./nginx/multi-unit-building"
NGINX_CONFIG_DEST="/etc/nginx/sites-available/multi-unit-building"
DOMAIN="multi-unit-building.goldenrabbit.biz"

echo "======================================================"
echo "  집합건물 세대별 정보 서비스 배포 스크립트"
echo "======================================================"
echo ""

# 루트 권한 확인
if [ "$EUID" -ne 0 ]; then
  echo "이 스크립트는 루트 권한으로 실행해야 합니다."
  echo "sudo ./deploy-script.sh 명령어로 다시 실행해주세요."
  exit 1
fi

# 서비스 디렉토리 생성 또는 확인
echo "서비스 디렉토리 준비 중..."
if [ ! -d "$TARGET_DIR" ]; then
  mkdir -p "$TARGET_DIR"
  mkdir -p "$TARGET_DIR/logs"
  mkdir -p "$TARGET_DIR/public"
  echo "디렉토리 생성 완료: $TARGET_DIR"
else
  echo "기존 디렉토리 발견: $TARGET_DIR"
fi

# 파일 복사
echo "애플리케이션 파일 복사 중..."
cp multi-unit-app.js "$TARGET_DIR/"
cp package.json "$TARGET_DIR/"

# .env 파일이 있으면 복사, 없으면 골든래빗 공통 .env 파일 사용
if [ -f ".env" ]; then
  cp .env "$TARGET_DIR/"
  echo ".env 파일 복사 완료"
else
  echo ".env 파일이 없습니다. 골든래빗 공통 .env 파일을 사용합니다."
fi

# public 디렉토리가 있으면 복사
if [ -d "public" ]; then
  cp -r public/* "$TARGET_DIR/public/"
  echo "public 파일 복사 완료"
fi

echo "파일 복사 완료"

# 의존성 설치
echo "Node.js 의존성 설치 중..."
cd "$TARGET_DIR"
npm install --production
echo "의존성 설치 완료"

# PM2 설치 확인 및 설치
if ! command -v pm2 &> /dev/null; then
  echo "PM2 설치 중..."
  npm install -g pm2
  echo "PM2 설치 완료"
else
  echo "PM2 이미 설치됨"
fi

# 서비스 시작 또는 재시작
echo "서비스 시작 중..."
if pm2 list | grep -q "$SERVICE_NAME"; then
  echo "기존 서비스 재시작 중..."
  pm2 restart "$SERVICE_NAME"
else
  echo "새 서비스 시작 중..."
  cd "$TARGET_DIR"
  pm2 start multi-unit-app.js --name "$SERVICE_NAME"
fi

# PM2 시작 프로그램 등록
echo "시스템 재시작 시 자동실행 등록 중..."
pm2 save
pm2 startup | tail -1 | bash

# Nginx 설정
echo "Nginx 설정 중..."
if [ -f "$NGINX_CONFIG_SRC" ]; then
  cp "$NGINX_CONFIG_SRC" "$NGINX_CONFIG_DEST"
  # sites-enabled에 심볼릭 링크 생성
  if [ ! -f "/etc/nginx/sites-enabled/multi-unit-building" ]; then
    ln -s "$NGINX_CONFIG_DEST" "/etc/nginx/sites-enabled/"
  fi
  # Nginx 설정 테스트
  nginx -t
  if [ $? -eq 0 ]; then
    echo "Nginx 설정 테스트 성공, 서비스 재시작 중..."
    systemctl restart nginx
    echo "Nginx 재시작 완료"
  else
    echo "Nginx 설정 테스트 실패. 수동으로 확인이 필요합니다."
  fi
else
  echo "Nginx 설정 파일을 찾을 수 없습니다: $NGINX_CONFIG_SRC"
  echo "수동으로 Nginx 설정을 진행해주세요."
fi

# 완료 메시지
echo ""
echo "======================================================"
echo "  배포가 완료되었습니다!"
echo "======================================================"
echo ""
echo "서비스 확인:"
echo "  - 웹 인터페이스: http://$DOMAIN"
echo "  - 상태 확인: http://$DOMAIN/health"
echo "  - 작업 수동 실행: http://$DOMAIN/run-job"
echo ""
echo "서비스 관리 명령어:"
echo "  - 서비스 상태 확인: pm2 status $SERVICE_NAME"
echo "  - 로그 확인: pm2 logs $SERVICE_NAME"
echo "  - 서비스 재시작: pm2 restart $SERVICE_NAME"
echo "  - 서비스 중지: pm2 stop $SERVICE_NAME"
echo ""
echo "환경 변수 설정 필요:"
echo "  - 집합건물 전용 Airtable 베이스: appQkFdB8TdPVNWdz"
echo "  - 집합건물 테이블 ID: tblT28nHoneqlbgBh"  
echo "  - 집합건물 뷰 ID: viwOs5jlYkIGPZ142"
echo "  - MULTI_UNIT_BUILDING_SERVICE_PORT: 서비스 포트 (기본값: 3002)"
echo "  - AIRTABLE_ACCESS_TOKEN: Airtable 접근 토큰"
echo "  - PUBLIC_API_KEY: 공공데이터포털 API 키"
echo "  - GOOGLE_SCRIPT_URL: 구글 스크립트 URL"
echo ""
echo "즐거운 하루 되세요!"
