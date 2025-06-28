require('dotenv').config({ path: '/root/goldenrabbit/.env' });
const express = require('express');
const cron = require('node-cron');
const axios = require('axios');
const Airtable = require('airtable');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.MULTI_UNIT_BUILDING_SERVICE_PORT || 3003;

// 로그 디렉토리 설정
const logDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

// 로그 레벨 설정
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

// 로그 레벨 우선순위 매핑
const LOG_LEVELS = {
  'debug': 0,
  'info': 1,
  'warn': 2,
  'error': 3
};

// 로그 파일에 저장하는 함수
function logToFile(level, message) {
  const now = new Date();
  const logFile = path.join(logDir, `${now.toISOString().split('T')[0]}.log`);
  const timestamp = now.toISOString();
  fs.appendFileSync(logFile, `[${timestamp}] [${level.toUpperCase()}] ${message}\n`);
}

// 로그 레벨에 따른 로깅 함수
function log(level, ...args) {
  if (LOG_LEVELS[level] < LOG_LEVELS[LOG_LEVEL]) {
    return;
  }

  const message = args.join(' ');

  if (level === 'error') {
    console.error(`[${level.toUpperCase()}]`, message);
  } else {
    console.log(`[${level.toUpperCase()}]`, message);
  }

  logToFile(level, message);
}

// 각 로그 레벨에 대한 편의 함수
const logger = {
  debug: (...args) => log('debug', ...args),
  info: (...args) => log('info', ...args),
  warn: (...args) => log('warn', ...args),
  error: (...args) => log('error', ...args)
};

// 로그 정리 함수 (7일 이상 된 로그 파일 삭제)
const cleanupLogs = () => {
  fs.readdir(logDir, (err, files) => {
    if (err) return logger.error('로그 정리 중 오류:', err);

    const now = new Date();
    let deletedCount = 0;

    files.forEach(file => {
      if (!file.endsWith('.log')) return;

      const filePath = path.join(logDir, file);
      const fileDate = new Date(file.split('.')[0]);
      const daysDiff = (now - fileDate) / (1000 * 60 * 60 * 24);

      if (daysDiff > 7) {
        fs.unlinkSync(filePath);
        deletedCount++;
      }
    });

    if (deletedCount > 0) {
      logger.info(`오래된 로그 파일 ${deletedCount}개 삭제 완료`);
    }
  });
};

// 매일 자정에 로그 정리 실행
cron.schedule('0 0 * * *', cleanupLogs);

// 에어테이블 설정 - 집합건물 전용 베이스
const airtableBase = new Airtable({
  apiKey: process.env.AIRTABLE_ACCESS_TOKEN || process.env.AIRTABLE_API_KEY
}).base('appQkFdB8TdPVNWdz'); // 집합건물 전용 베이스 ID

// 집합건물 정보용 테이블/뷰 - 새로운 뷰 사용
const MULTI_UNIT_TABLE = 'tblT28nHoneqlbgBh'; // 집합건물 테이블 ID
const MULTI_UNIT_VIEW = 'viwOs5jlYkIGPZ142'; // 새로운 뷰 ID

// API 키들
const PUBLIC_API_KEY = process.env.PUBLIC_API_KEY;
const GOOGLE_SCRIPT_URL = process.env.GOOGLE_SCRIPT_URL;

// API 요청 제한 설정 (과부하 방지)
const API_DELAY = 3000; // 3초 대기
const MAX_RETRIES = 2; // 최대 2회 재시도
const RETRY_DELAY = 5000; // 재시도 시 5초 대기

// 공통 함수: 주소 파싱 (개선된 버전)
const parseAddress = (address) => {
  if (!address || typeof address !== "string" || address.trim() === "") {
    return { error: "주소 없음", 원본주소: address || "입력값 없음" };
  }

  address = address.trim().replace(/\s+/g, ' ');

  // 동명 제거 (A동, 105동, 102동 등)
  address = address.replace(/\s+[A-Z]*\d*동\s+/, ' ');
  
  // 패턴 1: "구/시/군 법정동 번-지" 형태
  let match = address.match(/^(\S+구|\S+시|\S+군)\s+(\S+)\s+(\d+)-(\d+)$/);
  
  if (match) {
    const 시군구 = match[1];
    const 법정동 = match[2];
    const 번 = match[3].padStart(4, '0');
    const 지 = match[4].padStart(4, '0');
    
    logger.debug(`주소 파싱 성공 (번-지 형태): ${시군구} ${법정동} ${번}-${지}`);
    return { 시군구, 법정동, 번, 지 };
  }

  // 패턴 2: "구/시/군 법정동 번" 형태 (지번이 없는 경우)
  match = address.match(/^(\S+구|\S+시|\S+군)\s+(\S+)\s+(\d+)$/);
  
  if (match) {
    const 시군구 = match[1];
    const 법정동 = match[2];
    const 번 = match[3].padStart(4, '0');
    const 지 = "0000"; // 지번이 없으면 0000으로 설정
    
    logger.debug(`주소 파싱 성공 (번만 있는 형태): ${시군구} ${법정동} ${번}-${지}`);
    return { 시군구, 법정동, 번, 지 };
  }

  logger.error(`주소 파싱 실패: ${address}`);
  return { error: "잘못된 주소 형식", 원본주소: address };
};

// 처리가 필요한 레코드인지 검사하는 함수 (새로운 뷰 기반)
const needsProcessing = (record) => {
  try {
    // 필수 조건: 지번 주소와 호수가 있어야 함
    const 지번주소 = record.get('지번 주소');
    const 호수 = record.get('호수');

    if (!지번주소 || !호수) {
      logger.debug(`레코드 ${record.id}: 지번주소 또는 호수 없음 - 처리 불필요`);
      return false;
    }

    // 새로운 뷰에서는 이미 필터링되어 있으므로 현황 체크는 로깅용으로만
    const 현황원본 = record.get('현황');
    let 현황표시 = '없음';
    
    if (Array.isArray(현황원본)) {
      현황표시 = 현황원본.join(', ');
    } else if (typeof 현황원본 === 'string') {
      현황표시 = 현황원본;
    }

    logger.info(`레코드 ${record.id}: 현황 [${현황표시}] - 새로운 뷰에서 선택됨`);

    // 검사할 필드 목록 - 하나라도 비어있으면 처리 필요
    const 검사필드목록 = [
      '도로명주소',
      '전용면적(㎡)',
      '연면적(㎡)',
      '주구조',
      '주용도',
      '해당동 총층수',
      '해당동 승강기수',
      '총 세대/가구/호',
      '총주차대수',
      '주택가격(만원)',
      '사용승인일'
    ];

    for (const 필드 of 검사필드목록) {
      const 값 = record.get(필드);
      if (!값 || 값 === '' || 값 === null || 값 === undefined) {
        logger.debug(`레코드 ${record.id}: 필드 '${필드}'가 비어있음 - 처리 필요`);
        return true;
      }
    }

    logger.debug(`레코드 ${record.id}: 모든 필드가 채워져 있음 - 처리 불필요`);
    return false;
  } catch (error) {
    logger.error(`레코드 처리 필요성 검사 중 오류 (${record.id}):`, error);
    return false;
  }
};

// 지연 함수
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// 구글 스크립트를 통해 코드 가져오기 (재시도 로직 포함)
const getBuildingCodes = async (addressData) => {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      logger.debug(`구글 스크립트 호출 시도 ${attempt}/${MAX_RETRIES}:`, JSON.stringify(addressData));

      const response = await axios.post(
        GOOGLE_SCRIPT_URL || 'https://script.google.com/macros/s/AKfycbycxM4pNuDvzZp_iTsteqxWu738wMWfpPcLbzpHYNDD3CLg7oU1sFXycQfyZcounUDPVQ/exec',
        [addressData],
        {
          timeout: 30000,
          headers: { 'Content-Type': 'application/json' }
        }
      );

      // 응답 검증
      if (Array.isArray(response.data) && response.data.length > 0) {
        const data = response.data[0];
        if (data.시군구코드 !== undefined && data.법정동코드 !== undefined) {
          logger.info(`구글 스크립트 성공 (시도 ${attempt}): 시군구코드=${data.시군구코드}, 법정동코드=${data.법정동코드}`);
          return {
            ...addressData,
            시군구코드: String(data.시군구코드),
            법정동코드: String(data.법정동코드)
          };
        }
      }

      logger.warn(`구글 스크립트 응답에 유효한 코드 없음 (시도 ${attempt})`);
      
      if (attempt < MAX_RETRIES) {
        await delay(RETRY_DELAY);
      }
    } catch (error) {
      logger.error(`구글 스크립트 호출 실패 (시도 ${attempt}/${MAX_RETRIES}):`, error.message);
      
      if (attempt < MAX_RETRIES) {
        await delay(RETRY_DELAY);
      }
    }
  }

  throw new Error('구글 스크립트 호출 최종 실패');
};

// 건축물 표제부 정보 가져오기 (속도 제한 포함)
const getBuildingTitleInfo = async (codeData) => {
  try {
    logger.debug(`건축물 표제부 정보 조회 시작: ${codeData.id}`);

    const url = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo';
    const allItems = [];
    let pageNo = 1;
    const numOfRows = 50; // 한 번에 가져올 개수 줄임

    while (true) {
      // API 호출 전 대기
      await delay(API_DELAY);

      const response = await axios.get(url, {
        params: {
          serviceKey: PUBLIC_API_KEY,
          sigunguCd: codeData.시군구코드,
          bjdongCd: codeData.법정동코드,
          bun: codeData.번,
          ji: codeData.지,
          _type: 'json',
          numOfRows: numOfRows,
          pageNo: pageNo
        },
        headers: { accept: '*/*' },
        timeout: 30000
      });

      // 응답 검증
      if (!response.data?.response?.body) {
        logger.warn(`API 응답 구조 이상 (페이지 ${pageNo})`);
        break;
      }

      const body = response.data.response.body;

      // 에러 체크
      if (response.data.response.header?.resultCode !== '00') {
        const header = response.data.response.header;
        logger.error(`API 에러 응답: ${header.resultCode} - ${header.resultMsg}`);
        
        if (header.resultCode === '32') {
          logger.error('❌ API 호출량 초과 - 대기 시간 증가');
          await delay(10000); // 10초 대기
          continue;
        }
        break;
      }

      if (body.items && body.items.item) {
        const items = Array.isArray(body.items.item) ? body.items.item : [body.items.item];
        allItems.push(...items);
        
        logger.debug(`페이지 ${pageNo}: ${items.length}개 아이템 수집, 총 ${allItems.length}개`);

        if (items.length < numOfRows) {
          break;
        }
      } else {
        break;
      }

      pageNo++;
      
      // 최대 20페이지까지만
      if (pageNo > 20) {
        logger.warn(`최대 페이지 제한 도달 (20페이지)`);
        break;
      }
    }

    logger.info(`건축물 표제부 정보 수집 완료: 총 ${allItems.length}개 아이템`);

    return {
      response: {
        body: {
          items: { item: allItems }
        }
      },
      id: codeData.id
    };
  } catch (error) {
    logger.error(`건축물 표제부 정보 조회 실패 (${codeData.id}):`, error.message);
    return { body: {}, id: codeData.id };
  }
};

// 건축물 총괄표제부 정보 가져오기
const getBuildingRecapInfo = async (codeData) => {
  try {
    await delay(API_DELAY);
    
    const url = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo';
    const response = await axios.get(url, {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        _type: 'json',
        numOfRows: 10,
        pageNo: 1
      },
      headers: { accept: '*/*' },
      timeout: 30000
    });

    return { ...response.data, id: codeData.id };
  } catch (error) {
    logger.error(`건축물 총괄표제부 정보 조회 실패 (${codeData.id}):`, error.message);
    return { body: {}, id: codeData.id };
  }
};

// 건축물 전유공용면적 정보 가져오기
const getBuildingAreaInfo = async (codeData, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    const url = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo';
    const response = await axios.get(url, {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        dongNm: dongNm,
        hoNm: hoNm,
        _type: 'json',
        numOfRows: 50,
        pageNo: 1
      },
      headers: { accept: '*/*' },
      timeout: 30000
    });

    return { ...response.data, id: codeData.id };
  } catch (error) {
    logger.error(`건축물 면적 정보 조회 실패 (${codeData.id}):`, error.message);
    return { body: {}, id: codeData.id };
  }
};

// 지구지역 정보 가져오기
const getBuildingJijiguInfo = async (codeData) => {
  try {
    await delay(API_DELAY);
    
    const url = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrJijiguInfo';
    const response = await axios.get(url, {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        _type: 'json',
        numOfRows: 10,
        pageNo: 1
      },
      headers: { accept: '*/*' },
      timeout: 30000
    });

    return { ...response.data, id: codeData.id };
  } catch (error) {
    logger.error(`지구지역 정보 조회 실패 (${codeData.id}):`, error.message);
    return { body: {}, id: codeData.id };
  }
};

// 주택가격 정보 가져오기
const getBuildingHsprcInfo = async (codeData, mgmBldrgstPk) => {
  try {
    await delay(API_DELAY);
    
    const url = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrHsprcInfo';
    const allItems = [];
    let pageNo = 1;
    const numOfRows = 50;

    while (true) {
      const response = await axios.get(url, {
        params: {
          serviceKey: PUBLIC_API_KEY,
          sigunguCd: codeData.시군구코드,
          bjdongCd: codeData.법정동코드,
          bun: codeData.번,
          ji: codeData.지,
          mgmBldrgstPk: mgmBldrgstPk,
          _type: 'json',
          numOfRows: numOfRows,
          pageNo: pageNo
        },
        headers: { accept: '*/*' },
        timeout: 30000
      });

      if (response.data?.response?.body?.items?.item) {
        const items = Array.isArray(response.data.response.body.items.item) 
          ? response.data.response.body.items.item 
          : [response.data.response.body.items.item];
        
        allItems.push(...items);

        if (items.length < numOfRows) break;
      } else {
        break;
      }

      pageNo++;
      if (pageNo > 5) break; // 주택가격은 최대 5페이지까지만

      await delay(API_DELAY);
    }

    return {
      response: {
        body: {
          items: { item: allItems }
        }
      },
      id: codeData.id
    };
  } catch (error) {
    logger.error(`주택가격 정보 조회 실패 (${codeData.id}):`, error.message);
    return { body: {}, id: codeData.id };
  }
};

// 건축물 아이템 추출 및 처리
const extractBuildingItems = (data) => {
  try {
    if (!data?.response?.body?.items?.item) {
      return [];
    }

    const itemArray = data.response.body.items.item;
    if (!Array.isArray(itemArray)) {
      return [];
    }

    return itemArray.map(item => {
      if (item.platPlc) {
        item.platPlc = item.platPlc.replace(/^\S+\s/, '').replace(/번지$/, '');
      }
      return item;
    });
  } catch (error) {
    logger.error('건축물 아이템 추출 중 오류:', error);
    return [];
  }
};

// 건축물 데이터 처리 함수
const processMultiUnitBuildingData = (titleData, recapData, areaData, jijiguData, hsprcData, dongNm, hoNm) => {
  try {
    let processedData = {};

    // 표제부 정보 처리
    let titleItems = [];
    let mainInfo = null;
    let matchingDong = null;

    if (titleData?.response?.body?.items) {
      titleItems = extractBuildingItems(titleData);

      if (titleItems.length > 0) {
        mainInfo = titleItems[0];
        processedData["도로명주소"] = mainInfo.newPlatPlc || null;
        processedData["건물명"] = mainInfo.bldNm || null;

        // 동 매칭
        if (dongNm && dongNm.trim()) {
          matchingDong = titleItems.find(item => 
            item.dongNm && item.dongNm.trim() === dongNm.trim()
          );
        } else {
          matchingDong = titleItems.find(item => 
            item.mainAtchGbCdNm === "주건축물" && (!item.dongNm || item.dongNm.trim() === '')
          );
          if (!matchingDong) {
            matchingDong = titleItems.find(item => item.mainAtchGbCdNm === "주건축물");
          }
        }

        if (matchingDong) {
          const 해당동세대수 = parseInt(matchingDong.hhldCnt) || 0;
          const 해당동가구수 = parseInt(matchingDong.fmlyCnt) || 0;
          const 해당동호수 = parseInt(matchingDong.hoCnt) || 0;
          processedData["해당동 세대/가구/호"] = `${해당동세대수}/${해당동가구수}/${해당동호수}`;

          processedData["높이(m)"] = parseFloat(matchingDong.heit) || null;
          processedData["주용도"] = matchingDong.mainPurpsCdNm || null;
          processedData["주구조"] = matchingDong.strctCdNm || null;
          processedData["지붕"] = matchingDong.roofCdNm || null;
          processedData["해당동 총층수"] = parseInt(matchingDong.grndFlrCnt) || null;

          const 해당동승강기수 = (parseInt(matchingDong.rideUseElvtCnt) || 0) + (parseInt(matchingDong.emgenUseElvtCnt) || 0);
          processedData["해당동 승강기수"] = 해당동승강기수 > 0 ? 해당동승강기수 : null;

          // 사용승인일 변환
          if (matchingDong.useAprDay) {
            const formatDateISO = (dateStr) => {
              if (!dateStr || dateStr.length !== 8 || dateStr === "00000000") return null;
              const formattedDate = `${dateStr.substring(0, 4)}-${dateStr.substring(4, 6)}-${dateStr.substring(6, 8)}`;
              const date = new Date(`${formattedDate}T00:00:00.000Z`);
              return isNaN(date.getTime()) ? null : date.toISOString();
            };
            processedData["사용승인일"] = formatDateISO(matchingDong.useAprDay);
          }
        }
      }
    }

    // 총괄표제부 정보 처리 또는 표제부 정보로 대체
    const hasRecapData = recapData?.response?.body?.totalCount && parseInt(recapData.response.body.totalCount) > 0;
    
    if (hasRecapData) {
      // 총괄표제부가 있는 경우 (아파트 등)
      logger.debug('총괄표제부 데이터 사용 (아파트 등)');
      const recapItems = extractBuildingItems(recapData);
      if (recapItems.length > 0) {
        const recap = recapItems[0];

        processedData["대지면적(㎡)"] = parseFloat(recap.platArea) || null;
        processedData["건축면적(㎡)"] = parseFloat(recap.archArea) || null;
        processedData["건폐율(%)"] = parseFloat(recap.bcRat) || null;
        processedData["연면적(㎡)"] = parseFloat(recap.totArea) || null;
        processedData["용적률산정용연면적(㎡)"] = parseFloat(recap.vlRatEstmTotArea) || null;
        processedData["용적률(%)"] = parseFloat(recap.vlRat) || null;

        const 총세대수 = parseInt(recap.hhldCnt) || 0;
        const 총가구수 = parseInt(recap.fmlyCnt) || 0;
        const 총호수 = parseInt(recap.hoCnt) || 0;
        processedData["총 세대/가구/호"] = `${총세대수}/${총가구수}/${총호수}`;

        processedData["주건물수"] = parseInt(recap.mainBldCnt) || null;
        processedData["총주차대수"] = parseInt(recap.totPkngCnt) || null;
      }
    } else {
      // 총괄표제부가 없는 경우 (빌라, 다세대 등) - 표제부 정보로 대체
      logger.debug('총괄표제부 없음, 표제부 데이터로 대체 (빌라, 다세대 등)');
      
      if (mainInfo) {
        // 표제부의 정보를 총괄표제부 필드에 매핑
        processedData["대지면적(㎡)"] = parseFloat(mainInfo.platArea) || null;
        processedData["건축면적(㎡)"] = parseFloat(mainInfo.archArea) || null;
        processedData["건폐율(%)"] = parseFloat(mainInfo.bcRat) || null;
        processedData["연면적(㎡)"] = parseFloat(mainInfo.totArea) || null;
        processedData["용적률산정용연면적(㎡)"] = parseFloat(mainInfo.vlRatEstmTotArea) || null;
        processedData["용적률(%)"] = parseFloat(mainInfo.vlRat) || null;

        // 표제부의 세대/가구/호수 정보 사용
        const 총세대수 = parseInt(mainInfo.hhldCnt) || 0;
        const 총가구수 = parseInt(mainInfo.fmlyCnt) || 0;
        const 총호수 = parseInt(mainInfo.hoCnt) || 0;
        processedData["총 세대/가구/호"] = `${총세대수}/${총가구수}/${총호수}`;

        // 빌라의 경우 주건물수는 보통 1개
        processedData["주건물수"] = 1;

        // 표제부의 주차대수 정보 계산 (실내 + 실외)
        const 실내기계주차 = parseInt(mainInfo.indrMechUtcnt) || 0;
        const 실내자주주차 = parseInt(mainInfo.indrAutoUtcnt) || 0;
        const 실외기계주차 = parseInt(mainInfo.oudrMechUtcnt) || 0;
        const 실외자주주차 = parseInt(mainInfo.oudrAutoUtcnt) || 0;
        const 총주차대수 = 실내기계주차 + 실내자주주차 + 실외기계주차 + 실외자주주차;
        
        processedData["총주차대수"] = 총주차대수 > 0 ? 총주차대수 : null;

        logger.debug(`표제부 기반 총괄 정보: 세대수=${총세대수}, 가구수=${총가구수}, 호수=${총호수}, 주차=${총주차대수}`);
      }
    }

    // 면적 정보 처리
    let mgmBldrgstPk = null;
    if (areaData?.response?.body?.items) {
      const items = Array.isArray(areaData.response.body.items.item)
        ? areaData.response.body.items.item
        : [areaData.response.body.items.item];

      let 전용면적 = 0;
      let 공용면적 = 0;

      items.forEach(item => {
        const area = parseFloat(item.area) || 0;

        if (item.mainAtchGbCdNm === "주건축물" && item.exposPubuseGbCdNm === "전유" && item.mgmBldrgstPk) {
          mgmBldrgstPk = item.mgmBldrgstPk;
        }

        if (item.mainAtchGbCdNm === "주건축물" && item.exposPubuseGbCdNm === "전유") {
          전용면적 += area;
        } else if (item.mainAtchGbCdNm === "주건축물" && item.exposPubuseGbCdNm === "공용") {
          공용면적 += area;
        }
      });

      const 공급면적 = 전용면적 + 공용면적;
      processedData["전용면적(㎡)"] = 전용면적 > 0 ? 전용면적 : null;
      processedData["공급면적(㎡)"] = 공급면적 > 0 ? 공급면적 : null;
    }

    // 주택가격 정보 처리
    if (hsprcData?.response?.body?.items) {
      const hsprcItems = Array.isArray(hsprcData.response.body.items.item)
        ? hsprcData.response.body.items.item
        : [hsprcData.response.body.items.item];

      if (hsprcItems.length > 0) {
        const sortedItems = hsprcItems
          .filter(item => item.hsprc && item.crtnDay)
          .sort((a, b) => b.crtnDay.localeCompare(a.crtnDay));

        if (sortedItems.length > 0) {
          const latestPrice = sortedItems[0];
          const 주택가격원 = parseInt(latestPrice.hsprc) || 0;
          const 주택가격만원 = Math.round(주택가격원 / 10000);

          processedData["주택가격(만원)"] = 주택가격만원 > 0 ? 주택가격만원 : null;

          // 주택가격기준일을 ISO 형식으로 변환
          if (latestPrice.crtnDay) {
            const formatDateISO = (dateStr) => {
              if (!dateStr || dateStr.length !== 8) return null;
              const formattedDate = `${dateStr.substring(0, 4)}-${dateStr.substring(4, 6)}-${dateStr.substring(6, 8)}`;
              const date = new Date(`${formattedDate}T00:00:00.000Z`);
              return isNaN(date.getTime()) ? null : date.toISOString();
            };
            processedData["주택가격기준일"] = formatDateISO(latestPrice.crtnDay);
          }
        }
      }
    }

    return { processedData, mgmBldrgstPk };
  } catch (error) {
    logger.error('건축물 데이터 처리 중 오류:', error);
    return { processedData: {}, mgmBldrgstPk: null };
  }
};

// 에어테이블 업데이트 함수
const updateMultiUnitBuildingInfo = async (buildingData, recordId) => {
  try {
    const updateData = {};

    // 문제가 되는 필드들 제외
    const skipFields = [];

    Object.keys(buildingData).forEach(key => {
      const value = buildingData[key];

      if (skipFields.includes(key)) {
        logger.debug(`필드 ${key} 스킵: ${value}`);
        return;
      }

      if (value !== null && value !== undefined && value !== '') {
        updateData[key] = value;
      }
    });

    logger.debug(`업데이트할 데이터 (레코드 ${recordId}):`, JSON.stringify(updateData, null, 2));

    if (Object.keys(updateData).length === 0) {
      logger.warn(`업데이트할 유효한 데이터가 없음 (레코드 ${recordId})`);
      return false;
    }

    await airtableBase(MULTI_UNIT_TABLE).update(recordId, updateData);
    logger.info(`✅ 에어테이블 업데이트 성공: ${recordId}`);
    return true;
  } catch (error) {
    logger.error(`❌ 에어테이블 업데이트 실패 ${recordId}:`, error.message);
    return false;
  }
};

// 집합건물 레코드 처리 함수
const processMultiUnitBuildingRecord = async (record) => {
  try {
    const 지번주소 = record['지번 주소'];
    const 동 = record['동'] || '';
    const 호수 = record['호수'];
    const 현황 = Array.isArray(record['현황']) ? record['현황'].join(', ') : (record['현황'] || '없음');

    logger.info(`🏗️ 레코드 처리 시작: ${record.id} - ${지번주소} ${동} ${호수} (현황: ${현황})`);

    // 1단계: 주소 파싱
    const parsedAddress = parseAddress(지번주소);
    parsedAddress.id = record.id;

    if (parsedAddress.error) {
      logger.error(`❌ 주소 파싱 실패: ${parsedAddress.error}`);
      return false;
    }

    // 2단계: 건축물 코드 조회
    const buildingCodes = await getBuildingCodes(parsedAddress);

    // 3단계: 기본 건축물 데이터 조회 (직렬 처리)
    logger.info(`📡 API 데이터 수집 시작...`);
    
    const titleData = await getBuildingTitleInfo(buildingCodes);
    const recapData = await getBuildingRecapInfo(buildingCodes);
    const areaData = await getBuildingAreaInfo(buildingCodes, 동, 호수);
    const jijiguData = await getBuildingJijiguInfo(buildingCodes);

    // 4단계: 데이터 가공 및 mgmBldrgstPk 추출
    const { processedData, mgmBldrgstPk } = processMultiUnitBuildingData(
      titleData, recapData, areaData, jijiguData, null, 동, 호수
    );

    // 5단계: mgmBldrgstPk가 있으면 주택가격 정보 조회
    let finalProcessedData = processedData;
    if (mgmBldrgstPk) {
      logger.info(`💰 주택가격 정보 조회 중... (mgmBldrgstPk: ${mgmBldrgstPk})`);
      const hsprcData = await getBuildingHsprcInfo(buildingCodes, mgmBldrgstPk);
      
      const { processedData: finalData } = processMultiUnitBuildingData(
        titleData, recapData, areaData, jijiguData, hsprcData, 동, 호수
      );
      finalProcessedData = finalData;
    } else {
      logger.warn(`⚠️ mgmBldrgstPk를 찾을 수 없어 주택가격 정보를 건너뜁니다`);
    }

    if (Object.keys(finalProcessedData).length === 0) {
      logger.warn(`⚠️ 처리된 데이터가 없습니다: ${record.id}`);
      return false;
    }

    // 6단계: 에어테이블 업데이트
    const updated = await updateMultiUnitBuildingInfo(finalProcessedData, record.id);

    if (updated) {
      logger.info(`✅ 레코드 처리 완료: ${record.id}`);
    }

    return updated;
  } catch (error) {
    logger.error(`❌ 레코드 처리 실패 ${record.id}:`, error.message);
    return false;
  }
};

// 메인 작업 실행 함수
const runMultiUnitBuildingJob = async () => {
  try {
    logger.info('🚀 집합건물 정보 수집 작업 시작...');

    // 새로운 뷰에서 레코드 가져오기
    const allRecords = await airtableBase(MULTI_UNIT_TABLE)
      .select({
        view: MULTI_UNIT_VIEW
      })
      .all();

    logger.info(`📋 새로운 뷰에서 ${allRecords.length}개 레코드 발견`);

    // 처리가 필요한 레코드만 필터링
    const recordsToProcess = allRecords.filter(record => needsProcessing(record));

    logger.info(`🎯 처리 대상 레코드: ${recordsToProcess.length}개 (전체 ${allRecords.length}개 중)`);

    // 현황별 통계
    const 현황통계 = {};
    allRecords.forEach(record => {
      const 현황원본 = record.get('현황');
      let 현황표시 = '없음';
      
      if (Array.isArray(현황원본)) {
        현황표시 = 현황원본.join(', ');
      } else if (typeof 현황원본 === 'string') {
        현황표시 = 현황원본;
      }
      
      현황통계[현황표시] = (현황통계[현황표시] || 0) + 1;
    });
    
    logger.info('📊 현황별 레코드 수:', 현황통계);

    if (recordsToProcess.length === 0) {
      logger.info('✅ 처리할 레코드가 없습니다');
      return { total: 0, success: 0 };
    }

    // 레코드 정보 추출
    const recordData = recordsToProcess.map(record => {
      const 현황원본 = record.get('현황');
      let 현황표시 = '없음';
      
      if (Array.isArray(현황원본)) {
        현황표시 = 현황원본.join(', ');
      } else if (typeof 현황원본 === 'string') {
        현황표시 = 현황원본;
      }

      return {
        id: record.id,
        '지번 주소': record.get('지번 주소') || '',
        '동': record.get('동') || '',
        '호수': record.get('호수') || '',
        '현황': 현황표시
      };
    });

    logger.info('📝 처리할 레코드 목록:');
    recordData.forEach((record, index) => {
      logger.info(`  ${index + 1}. ${record.id}: ${record['지번 주소']} ${record['동']} ${record['호수']} (현황: ${record['현황']})`);
    });

    // 직렬 처리 (API 과부하 방지)
    logger.info(`⏳ 직렬 처리 시작 (총 ${recordData.length}개, 각 레코드마다 ${API_DELAY/1000}초 대기)...`);

    const results = [];
    for (let i = 0; i < recordData.length; i++) {
      const record = recordData[i];
      
      try {
        logger.info(`\n📍 [${i + 1}/${recordData.length}] 처리 중: ${record.id}`);
        logger.info(`   주소: ${record['지번 주소']} ${record['동']} ${record['호수']}`);
        logger.info(`   현황: ${record['현황']}`);

        const success = await processMultiUnitBuildingRecord(record);

        results.push({
          id: record.id,
          success: success,
          index: i + 1
        });

        if (success) {
          logger.info(`✅ [${i + 1}/${recordData.length}] 성공: ${record.id}`);
        } else {
          logger.warn(`❌ [${i + 1}/${recordData.length}] 실패: ${record.id}`);
        }

        // 마지막 레코드가 아니면 추가 대기
        if (i < recordData.length - 1) {
          logger.info(`⏸️ 다음 레코드 처리까지 ${API_DELAY/1000}초 대기...`);
          await delay(API_DELAY);
        }

      } catch (error) {
        logger.error(`❌ 레코드 처리 중 예외 발생 ${record.id}:`, error.message);
        results.push({
          id: record.id,
          success: false,
          index: i + 1,
          error: error.message
        });
      }
    }

    // 결과 집계
    const successCount = results.filter(r => r.success).length;
    const failedCount = results.length - successCount;

    // 실패한 레코드들 로깅
    const failedRecords = results.filter(r => !r.success);
    if (failedRecords.length > 0) {
      logger.warn(`❌ 실패한 레코드들: ${failedRecords.map(r => r.id).join(', ')}`);
    }

    logger.info(`\n🎉 작업 완료!`);
    logger.info(`📊 처리 결과: ${recordData.length}개 중 ${successCount}개 성공, ${failedCount}개 실패`);
    logger.info(`📈 성공률: ${((successCount / recordData.length) * 100).toFixed(1)}%`);

    return { total: recordData.length, success: successCount, failed: failedCount };
  } catch (error) {
    logger.error('❌ 작업 실행 중 오류:', error.message);
    return { total: 0, success: 0, error: error.message };
  }
};

// 스케줄링 - 1분마다 실행
cron.schedule('* * * * *', async () => {
  logger.debug('🔍 작업 확인 중...');

  try {
    // 처리할 레코드가 있는지 빠르게 확인 (최대 5개만 체크)
    const sampleRecords = await airtableBase(MULTI_UNIT_TABLE)
      .select({
        view: MULTI_UNIT_VIEW,
        maxRecords: 5
      })
      .all();

    // 처리가 필요한 레코드가 있는지 확인
    const needsWork = sampleRecords.some(record => needsProcessing(record));

    if (needsWork) {
      logger.info('🎯 처리할 집합건물 레코드 발견, 작업 실행 중...');
      await runMultiUnitBuildingJob();
    } else {
      logger.debug('✅ 처리할 집합건물 레코드 없음, 작업 건너뜀');
    }
  } catch (error) {
    logger.error('❌ 작업 확인 중 오류 발생:', error.message);
  }
});

// 미들웨어 설정
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 정적 파일 제공
app.use(express.static(path.join(__dirname, 'public')));

// ===== API 엔드포인트들 =====

// 상태 확인 엔드포인트
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    service: 'multi-unit-building-service',
    timestamp: new Date().toISOString(),
    version: '2.0.0',
    viewId: MULTI_UNIT_VIEW
  });
});

// 수동 작업 실행 엔드포인트
app.get('/run-job', async (req, res) => {
  try {
    logger.info('🔧 수동 작업 실행 요청:', new Date().toISOString());
    const result = await runMultiUnitBuildingJob();
    res.status(200).json({
      message: '집합건물 작업 완료',
      result
    });
  } catch (error) {
    logger.error('❌ 수동 작업 실행 실패:', error);
    res.status(500).json({
      error: '집합건물 작업 실행 실패',
      details: error.message
    });
  }
});

// 뷰 정보 확인 엔드포인트
app.get('/view-info', async (req, res) => {
  try {
    const allRecords = await airtableBase(MULTI_UNIT_TABLE)
      .select({
        view: MULTI_UNIT_VIEW,
        maxRecords: 20
      })
      .all();

    const recordsInfo = allRecords.map(record => {
      const 현황원본 = record.get('현황');
      let 현황표시 = '없음';
      
      if (Array.isArray(현황원본)) {
        현황표시 = 현황원본.join(', ');
      } else if (typeof 현황원본 === 'string') {
        현황표시 = 현황원본;
      }

      return {
        id: record.id,
        지번주소: record.get('지번 주소'),
        호수: record.get('호수'),
        현황: 현황표시,
        needsProcessing: needsProcessing(record)
      };
    });

    const needsProcessingCount = recordsInfo.filter(r => r.needsProcessing).length;

    // 현황별 통계
    const 현황통계 = {};
    recordsInfo.forEach(record => {
      const 현황 = record.현황 || '없음';
      현황통계[현황] = (현황통계[현황] || 0) + 1;
    });

    res.json({
      viewId: MULTI_UNIT_VIEW,
      totalRecords: allRecords.length,
      needsProcessing: needsProcessingCount,
      현황통계,
      sampleRecords: recordsInfo
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 단일 레코드 테스트 엔드포인트
app.get('/test-record/:recordId', async (req, res) => {
  try {
    const recordId = req.params.recordId;
    
    const record = await airtableBase(MULTI_UNIT_TABLE).find(recordId);
    
    const 지번주소 = record.get('지번 주소');
    const 동 = record.get('동');
    const 호수 = record.get('호수');
    const 현황원본 = record.get('현황');

    const recordData = {
      id: recordId,
      '지번 주소': 지번주소,
      '동': 동 || '',
      '호수': 호수,
      '현황': 현황원본
    };

    const success = await processMultiUnitBuildingRecord(recordData);

    res.json({
      recordId,
      recordData,
      success,
      message: success ? '처리 성공' : '처리 실패'
    });
  } catch (error) {
    logger.error('단일 레코드 테스트 실패:', error);
    res.status(500).json({ error: error.message });
  }
});

// API 상태 확인 엔드포인트
app.get('/api-status', async (req, res) => {
  try {
    const testUrl = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo';
    const testParams = {
      serviceKey: PUBLIC_API_KEY,
      sigunguCd: '11680', // 강남구
      bjdongCd: '10600',  // 역삼동
      bun: '0001',
      ji: '0000',
      _type: 'json',
      numOfRows: 1,
      pageNo: 1
    };

    const response = await axios.get(testUrl, {
      params: testParams,
      timeout: 30000
    });

    const isValid = response.data?.response?.header?.resultCode === '00';

    res.json({
      apiKeyValid: isValid,
      responseCode: response.data?.response?.header?.resultCode,
      responseMessage: response.data?.response?.header?.resultMsg,
      hasApiKey: !!PUBLIC_API_KEY,
      apiKeyLength: PUBLIC_API_KEY ? PUBLIC_API_KEY.length : 0
    });
  } catch (error) {
    res.status(500).json({
      apiKeyValid: false,
      error: error.message,
      hasApiKey: !!PUBLIC_API_KEY
    });
  }
});

// 간단한 웹 인터페이스 제공
app.get('/', (req, res) => {
  res.send(`
    <html>
    <head>
        <title>집합건물 서비스 관리</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .button { display: inline-block; padding: 10px 20px; margin: 10px; 
                     background: #007bff; color: white; text-decoration: none; 
                     border-radius: 5px; }
            .button:hover { background: #0056b3; }
            .info { background: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0; }
        </style>
    </head>
    <body>
        <h1>🏗️ 집합건물 서비스 관리</h1>
        
        <div class="info">
            <h3>📋 현재 설정</h3>
            <p><strong>뷰 ID:</strong> ${MULTI_UNIT_VIEW}</p>
            <p><strong>대상 현황:</strong> 네이버, 디스코, 당근, 등록대기</p>
            <p><strong>API 지연시간:</strong> ${API_DELAY/1000}초</p>
            <p><strong>스케줄:</strong> 1분마다 실행</p>
        </div>

        <h3>🔧 관리 기능</h3>
        <a href="/health" class="button">상태 확인</a>
        <a href="/view-info" class="button">뷰 정보 확인</a>
        <a href="/api-status" class="button">API 상태 확인</a>
        <a href="/run-job" class="button">수동 작업 실행</a>

        <h3>📊 모니터링</h3>
        <p>로그 확인: <code>pm2 logs multi-unit-building-service</code></p>
        <p>프로세스 상태: <code>pm2 status</code></p>
    </body>
    </html>
  `);
});

// 서버 시작
app.listen(PORT, () => {
  logger.info('🚀 집합건물 서비스 시작됨');
  logger.info(`📡 포트: ${PORT}`);
  logger.info(`🌐 웹 인터페이스: http://localhost:${PORT}`);
  logger.info(`📋 사용 뷰: ${MULTI_UNIT_VIEW}`);
  logger.info(`⏱️ API 지연시간: ${API_DELAY/1000}초`);
  logger.info(`🔄 스케줄: 1분마다 실행`);
});

