logger.info('🚀 집합건물 서비스 v3.3 시작됨');require('dotenv').config({ path: '/root/goldenrabbit/.env' });
const express = require('express');
const cron = require('node-cron');
const axios = require('axios');
const Airtable = require('airtable');
const convert = require('xml-js');
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
const LOG_LEVELS = {
  'debug': 0,
  'info': 1,
  'warn': 2,
  'error': 3
};

// 로그 함수
function logToFile(level, message) {
  const now = new Date();
  const logFile = path.join(logDir, `${now.toISOString().split('T')[0]}.log`);
  const timestamp = now.toISOString();
  fs.appendFileSync(logFile, `[${timestamp}] [${level.toUpperCase()}] ${message}\n`);
}

function log(level, ...args) {
  if (LOG_LEVELS[level] < LOG_LEVELS[LOG_LEVEL]) return;
  
  const message = args.join(' ');
  if (level === 'error') {
    console.error(`[${level.toUpperCase()}]`, message);
  } else {
    console.log(`[${level.toUpperCase()}]`, message);
  }
  logToFile(level, message);
}

const logger = {
  debug: (...args) => log('debug', ...args),
  info: (...args) => log('info', ...args),
  warn: (...args) => log('warn', ...args),
  error: (...args) => log('error', ...args)
};

// 에어테이블 설정
const airtableBase = new Airtable({
  apiKey: process.env.AIRTABLE_ACCESS_TOKEN || process.env.AIRTABLE_API_KEY
}).base('appQkFdB8TdPVNWdz');

const MULTI_UNIT_TABLE = 'tblT28nHoneqlbgBh';
const MULTI_UNIT_VIEW = 'viwOs5jlYkIGPZ142';

// API 설정
const PUBLIC_API_KEY = process.env.PUBLIC_API_KEY;
const GOOGLE_SCRIPT_URL = process.env.GOOGLE_SCRIPT_URL;
const VWORLD_APIKEY = process.env.VWORLD_APIKEY;

const API_DELAY = 2000; // 2초 대기
const MAX_RETRIES = 2;
const RETRY_DELAY = 3000;

// 유틸리티 함수들
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const parseAddress = (address) => {
  if (!address || typeof address !== "string" || address.trim() === "") {
    return { error: "주소 없음", 원본주소: address || "입력값 없음" };
  }

  address = address.trim().replace(/\s+/g, ' ').replace(/\s+[A-Z]*\d*동\s+/, ' ');
  
  let match = address.match(/^(\S+구|\S+시|\S+군)\s+(\S+)\s+(\d+)-(\d+)$/);
  if (match) {
    return { 
      시군구: match[1], 
      법정동: match[2], 
      번: match[3].padStart(4, '0'), 
      지: match[4].padStart(4, '0') 
    };
  }

  match = address.match(/^(\S+구|\S+시|\S+군)\s+(\S+)\s+(\d+)$/);
  if (match) {
    return { 
      시군구: match[1], 
      법정동: match[2], 
      번: match[3].padStart(4, '0'), 
      지: "0000" 
    };
  }

  return { error: "잘못된 주소 형식", 원본주소: address };
};

const getBuildingCodes = async (addressData) => {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await axios.post(GOOGLE_SCRIPT_URL, [addressData], {
        timeout: 30000,
        headers: { 'Content-Type': 'application/json' }
      });

      if (Array.isArray(response.data) && response.data.length > 0) {
        const data = response.data[0];
        if (data.시군구코드 !== undefined && data.법정동코드 !== undefined) {
          return {
            ...addressData,
            시군구코드: String(data.시군구코드),
            법정동코드: String(data.법정동코드)
          };
        }
      }

      if (attempt < MAX_RETRIES) await delay(RETRY_DELAY);
    } catch (error) {
      logger.error(`구글 스크립트 호출 실패 (시도 ${attempt}):`, error.message);
      if (attempt < MAX_RETRIES) await delay(RETRY_DELAY);
    }
  }
  throw new Error('구글 스크립트 호출 최종 실패');
};

const generatePNU = (codeData) => {
  if (!codeData.시군구코드 || !codeData.법정동코드 || !codeData.번 || !codeData.지) {
    return null;
  }
  return `${codeData.시군구코드}${codeData.법정동코드}1${codeData.번}${codeData.지}`;
};

const formatDateISO = (dateStr) => {
  if (!dateStr || dateStr.length !== 8 || dateStr === "00000000") return null;
  const formattedDate = `${dateStr.substring(0, 4)}-${dateStr.substring(4, 6)}-${dateStr.substring(6, 8)}`;
  const date = new Date(`${formattedDate}T00:00:00.000Z`);
  return isNaN(date.getTime()) ? null : date.toISOString();
};

// API 호출 함수들
const getBuildingRecapInfo = async (codeData) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo', {
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
      timeout: 30000
    });

    return response.data;
  } catch (error) {
    logger.error('getBuildingRecapInfo 실패:', error.message);
    return null;
  }
};

const getBuildingTitleInfo = async (codeData) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo', {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        _type: 'json',
        numOfRows: 50,
        pageNo: 1
      },
      timeout: 30000
    });

    return response.data;
  } catch (error) {
    logger.error('getBuildingTitleInfo 실패:', error.message);
    return null;
  }
};

const getBuildingAreaInfo = async (codeData, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo', {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        dongNm: dongNm || '',
        hoNm: hoNm || '',
        _type: 'json',
        numOfRows: 50,
        pageNo: 1
      },
      timeout: 30000
    });

    return response.data;
  } catch (error) {
    logger.error('getBuildingAreaInfo 실패:', error.message);
    return null;
  }
};

const getBuildingExposInfo = async (codeData, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposInfo', {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        dongNm: dongNm || '',
        hoNm: hoNm || '',
        _type: 'json',
        numOfRows: 50,
        pageNo: 1
      },
      timeout: 30000
    });

    return response.data;
  } catch (error) {
    logger.error('getBuildingExposInfo 실패:', error.message);
    return null;
  }
};

const getBuildingHsprcInfo = async (codeData, mgmBldrgstPk) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrHsprcInfo', {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        mgmBldrgstPk: mgmBldrgstPk,
        _type: 'json',
        numOfRows: 50,
        pageNo: 1
      },
      timeout: 30000
    });

    return response.data;
  } catch (error) {
    logger.error('getBuildingHsprcInfo 실패:', error.message);
    return null;
  }
};

// VWorld API를 사용한 토지특성 정보 조회 (용도지역, 토지면적)
const getLandCharacteristics = async (pnu) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://api.vworld.kr/ned/data/getLandCharacteristics', {
      params: {
        key: VWORLD_APIKEY,
        domain: 'localhost',
        pnu: pnu,
        stdrYear: '2024',
        format: 'xml',
        numOfRows: 10,
        pageNo: 1
      },
      timeout: 30000
    });

    const jsonData = convert.xml2js(response.data, { compact: true, spaces: 2, textKey: '_text' });
    
    if (jsonData && jsonData.response && jsonData.response.fields && jsonData.response.fields.field) {
      let fields = jsonData.response.fields.field;
      if (!Array.isArray(fields)) fields = [fields];
      
      if (fields.length > 0) {
        const field = fields[0];
        
        return {
          용도지역: field.prposArea1Nm && field.prposArea1Nm._text ? field.prposArea1Nm._text : null,
          토지면적: field.lndpclAr && field.lndpclAr._text ? parseFloat(field.lndpclAr._text) : null
        };
      }
    }
    
    return { 용도지역: null, 토지면적: null };
  } catch (error) {
    logger.error('getLandCharacteristics 실패:', error.message);
    return { 용도지역: null, 토지면적: null };
  }
};

// VWorld API를 사용한 대지지분 정보 조회
const getLandShareInfo = async (pnu, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://api.vworld.kr/ned/data/ldaregList', {
      params: {
        key: VWORLD_APIKEY,
        domain: 'localhost',
        pnu: pnu,
        buldDongNm: dongNm || '',
        buldHoNm: hoNm || '',
        format: 'xml',
        numOfRows: 10,
        pageNo: 1
      },
      timeout: 30000
    });

    const jsonData = convert.xml2js(response.data, { compact: true, spaces: 2, textKey: '_text' });
    
    if (jsonData && jsonData.response && jsonData.response.fields && jsonData.response.fields.field) {
      let fields = jsonData.response.fields.field;
      if (!Array.isArray(fields)) fields = [fields];
      
      for (const field of fields) {
        if (field.ldaQotaRate && field.ldaQotaRate._text) {
          const quotaRate = field.ldaQotaRate._text;
          const shareValue = parseFloat(quotaRate.split('/')[0]);
          if (!isNaN(shareValue)) {
            return shareValue;
          }
        }
      }
    }
    
    return null;
  } catch (error) {
    logger.error('getLandShareInfo 실패:', error.message);
    return null;
  }
};

// 데이터 처리 함수들
const extractItems = (data) => {
  if (!data || !data.response || !data.response.body || !data.response.body.items || !data.response.body.items.item) {
    return [];
  }
  
  const items = data.response.body.items.item;
  return Array.isArray(items) ? items : [items];
};

// 동/호수 매칭을 위한 정규화 함수
const normalizeDongHo = (value) => {
  if (!value || typeof value !== 'string') return '';
  
  // 숫자만 추출 (102동 -> 102, 1003호 -> 1003)
  const numbers = value.replace(/[^0-9]/g, '');
  return numbers;
};

// 동/호수 매칭 함수 개선
const isDongMatch = (apiDong, inputDong) => {
  if (!inputDong || inputDong.trim() === '') return true; // 입력 동이 없으면 매칭
  
  const normalizedInput = normalizeDongHo(inputDong);
  const normalizedApi = normalizeDongHo(apiDong || '');
  
  // 정규화된 숫자가 일치하면 매칭
  return normalizedInput === normalizedApi;
};

const isHoMatch = (apiHo, inputHo) => {
  if (!inputHo) return false;
  
  const normalizedInput = normalizeDongHo(inputHo);
  const normalizedApi = normalizeDongHo(apiHo || '');
  
  return normalizedInput === normalizedApi;
};

const findMgmBldrgstPk = (exposData, dongNm, hoNm) => {
  const items = extractItems(exposData);
  
  for (const item of items) {
    if (dongNm && dongNm.trim()) {
      // 동이 있는 경우: 동과 호수가 일치하고 주건축물+전유인 경우
      if (isDongMatch(item.dongNm, dongNm) && 
          isHoMatch(item.hoNm, hoNm) &&
          item.mainAtchGbCdNm === "주건축물" && 
          item.exposPubuseGbCdNm === "전유") {
        return item.mgmBldrgstPk;
      }
    } else {
      // 동이 없는 경우: 호수만 일치하는 경우
      if (isHoMatch(item.hoNm, hoNm) && 
          item.mainAtchGbCdNm === "주건축물" && 
          item.exposPubuseGbCdNm === "전유") {
        return item.mgmBldrgstPk;
      }
    }
  }
  
  return null;
};

const processMultiUnitBuildingData = (recapData, titleData, areaData, landCharacteristics, hsprcData, landShare, dongNm, hoNm) => {
  const result = {};
  
  // 총괄표제부 데이터가 있는지 확인
  const hasRecapData = recapData && recapData.response && recapData.response.body && recapData.response.body.totalCount && parseInt(recapData.response.body.totalCount) > 0;
  
  if (hasRecapData) {
    // === getBrRecapTitleInfo가 있는 경우 ===
    logger.info('총괄표제부 데이터 처리 중 (아파트 등)');
    
    const recapItems = extractItems(recapData);
    if (recapItems.length > 0) {
      const recap = recapItems[0];
      
      // 1. 총괄표제부에서 기본 정보 (면적/비율/수량은 숫자로 처리)
      if (recap.platArea) result["대지면적(㎡)"] = parseFloat(recap.platArea);
      if (recap.totArea) result["연면적(㎡)"] = parseFloat(recap.totArea);
      if (recap.vlRatEstmTotArea) result["용적률산정용연면적(㎡)"] = parseFloat(recap.vlRatEstmTotArea);
      if (recap.archArea) result["건축면적(㎡)"] = parseFloat(recap.archArea);
      if (recap.bcRat) result["건폐율(%)"] = parseFloat(recap.bcRat);
      if (recap.vlRat) result["용적률(%)"] = parseFloat(recap.vlRat);
      if (recap.bldNm) result["건물명"] = recap.bldNm;
      if (recap.totPkngCnt) result["총주차대수"] = parseInt(recap.totPkngCnt);
      if (recap.useAprDay) result["사용승인일"] = formatDateISO(recap.useAprDay);
      
      const 총세대수 = recap.hhldCnt || '0';
      const 총가구수 = recap.fmlyCnt || '0';
      const 총호수 = recap.hoCnt || '0';
      result["총 세대/가구/호"] = `${총세대수}/${총가구수}/${총호수}`;
      
      if (recap.mainBldCnt) result["주건물수"] = parseInt(recap.mainBldCnt);
    }
    
    // 2. 표제부에서 해당 동 정보 (동 매칭 로직 개선)
    const titleItems = extractItems(titleData);
    if (titleItems.length > 0) {
      let matchingDong = null;
      
      if (dongNm && dongNm.trim()) {
        matchingDong = titleItems.find(item => isDongMatch(item.dongNm, dongNm));
      } else {
        matchingDong = titleItems.find(item => item.mainAtchGbCdNm === "주건축물");
      }
      
      if (matchingDong) {
        if (matchingDong.heit) result["높이(m)"] = parseFloat(matchingDong.heit);
        if (matchingDong.strctCdNm) result["주구조"] = matchingDong.strctCdNm;
        if (matchingDong.roofCdNm) result["지붕"] = matchingDong.roofCdNm;
        if (matchingDong.mainPurpsCdNm) result["주용도"] = matchingDong.mainPurpsCdNm;
        
        // 총층수를 -지하층수/지상층수 형태로 변환
        const 지상층수 = matchingDong.grndFlrCnt || '0';
        const 지하층수 = matchingDong.ugrndFlrCnt || '0';
        result["총층수"] = `-${지하층수}/${지상층수}`;
        
        const 해당동세대수 = matchingDong.hhldCnt || '0';
        const 해당동가구수 = matchingDong.fmlyCnt || '0';
        const 해당동호수 = matchingDong.hoCnt || '0';
        result["해당동 세대/가구/호"] = `${해당동세대수}/${해당동가구수}/${해당동호수}`;
        
        const 승강기수1 = parseInt(matchingDong.rideUseElvtCnt) || 0;
        const 승강기수2 = parseInt(matchingDong.emgenUseElvtCnt) || 0;
        const 총승강기수 = 승강기수1 + 승강기수2;
        if (총승강기수 > 0) result["해당동 승강기수"] = 총승강기수;
      }
    }
    
  } else {
    // === getBrRecapTitleInfo가 없는 경우 ===
    logger.info('총괄표제부 없음, 표제부 데이터 처리 중 (빌라, 다세대 등)');
    
    const titleItems = extractItems(titleData);
    if (titleItems.length > 0) {
      const mainInfo = titleItems[0];
      
      // 1. 표제부에서 모든 정보 (면적/비율/수량은 숫자로 처리)
      if (mainInfo.newPlatPlc) result["도로명주소"] = mainInfo.newPlatPlc;
      if (mainInfo.heit) result["높이(m)"] = parseFloat(mainInfo.heit);
      if (mainInfo.strctCdNm) result["주구조"] = mainInfo.strctCdNm;
      if (mainInfo.roofCdNm) result["지붕"] = mainInfo.roofCdNm;
      if (mainInfo.mainPurpsCdNm) result["주용도"] = mainInfo.mainPurpsCdNm;
      
      // 총층수를 -지하층수/지상층수 형태로 변환
      const 지상층수 = mainInfo.grndFlrCnt || '0';
      const 지하층수 = mainInfo.ugrndFlrCnt || '0';
      result["총층수"] = `-${지하층수}/${지상층수}`;
      
      const 세대수 = mainInfo.hhldCnt || '0';
      const 가구수 = mainInfo.fmlyCnt || '0';
      const 호수 = mainInfo.hoCnt || '0';
      result["해당동 세대/가구/호"] = `${세대수}/${가구수}/${호수}`;
      
      const 주차1 = parseInt(mainInfo.indrMechUtcnt) || 0;
      const 주차2 = parseInt(mainInfo.oudrMechUtcnt) || 0;
      const 주차3 = parseInt(mainInfo.indrAutoUtcnt) || 0;
      const 주차4 = parseInt(mainInfo.oudrAutoUtcnt) || 0;
      const 총주차대수 = 주차1 + 주차2 + 주차3 + 주차4;
      if (총주차대수 > 0) result["총주차대수"] = 총주차대수;
      
      const 승강기수1 = parseInt(mainInfo.rideUseElvtCnt) || 0;
      const 승강기수2 = parseInt(mainInfo.emgenUseElvtCnt) || 0;
      const 총승강기수 = 승강기수1 + 승강기수2;
      if (총승강기수 > 0) result["해당동 승강기수"] = 총승강기수;
    }
  }
  
  // 3. 면적 정보 (공통) - 숫자로 처리
  if (areaData) {
    const areaItems = extractItems(areaData);
    let 전용면적 = 0;
    let 공용면적 = 0;
    
    areaItems.forEach(item => {
      const area = parseFloat(item.area) || 0;
      if (item.mainAtchGbCdNm === "주건축물" && item.exposPubuseGbCdNm === "전유") {
        전용면적 += area;
      } else if (item.mainAtchGbCdNm === "주건축물" && item.exposPubuseGbCdNm === "공용") {
        공용면적 += area;
      }
    });
    
    if (전용면적 > 0) result["전용면적(㎡)"] = 전용면적;
    if ((전용면적 + 공용면적) > 0) result["공급면적(㎡)"] = 전용면적 + 공용면적;
  }
  
  // 4. VWorld 토지특성 정보 (용도지역, 토지면적)
  if (landCharacteristics) {
    if (landCharacteristics.용도지역) {
      result["용도지역"] = landCharacteristics.용도지역;
    }
    if (landCharacteristics.토지면적) {
      result["토지면적(㎡)"] = landCharacteristics.토지면적; // 숫자로 처리
    }
  }
  
  // 5. 주택가격 정보 (공통) - 숫자로 처리
  if (hsprcData) {
    const hsprcItems = extractItems(hsprcData);
    if (hsprcItems.length > 0) {
      const sortedItems = hsprcItems
        .filter(item => item.hsprc && item.crtnDay)
        .sort((a, b) => b.crtnDay.localeCompare(a.crtnDay));
      
      if (sortedItems.length > 0) {
        const latestPrice = sortedItems[0];
        const 주택가격원 = parseInt(latestPrice.hsprc) || 0;
        const 주택가격만원 = Math.round(주택가격원 / 10000);
        
        result["주택가격(만원)"] = 주택가격만원; // 숫자로 처리
        if (latestPrice.crtnDay) result["주택가격기준일"] = formatDateISO(latestPrice.crtnDay);
      }
    } else {
      result["주택가격(만원)"] = 0;
    }
  } else {
    result["주택가격(만원)"] = 0;
  }
  
  // 6. 대지지분 정보 (공통) - 숫자로 처리
  if (landShare !== null) {
    result["대지지분(㎡)"] = landShare; // 이미 parseFloat로 처리된 숫자
  }
  
  return result;
};

// 메인 처리 함수
const processMultiUnitBuildingRecord = async (record) => {
  try {
    const 지번주소 = record['지번 주소'];
    const 동 = record['동'] || '';
    const 호수 = record['호수'];

    logger.info(`🏗️ 레코드 처리 시작: ${record.id} - ${지번주소} ${동} ${호수}`);

    // 1. 주소 파싱
    const parsedAddress = parseAddress(지번주소);
    if (parsedAddress.error) {
      logger.error(`주소 파싱 실패: ${parsedAddress.error}`);
      return false;
    }

    // 2. 건축물 코드 조회
    const buildingCodes = await getBuildingCodes(parsedAddress);
    
    // 3. PNU 생성 (VWorld API용)
    const pnu = generatePNU(buildingCodes);

    // 4. API 데이터 수집
    logger.info(`📡 API 데이터 수집 시작...`);
    
    const recapData = await getBuildingRecapInfo(buildingCodes);
    const titleData = await getBuildingTitleInfo(buildingCodes);
    const areaData = await getBuildingAreaInfo(buildingCodes, 동, 호수);
    const exposData = await getBuildingExposInfo(buildingCodes, 동, 호수);
    
    let landCharacteristics = null;
    let landShare = null;
    
    if (pnu) {
      landCharacteristics = await getLandCharacteristics(pnu);
      landShare = await getLandShareInfo(pnu, 동, 호수);
    }

    // 5. mgmBldrgstPk 추출
    const mgmBldrgstPk = findMgmBldrgstPk(exposData, 동, 호수);
    
    // 6. 주택가격 정보 조회
    let hsprcData = null;
    if (mgmBldrgstPk) {
      logger.info(`💰 주택가격 정보 조회 중... (mgmBldrgstPk: ${mgmBldrgstPk})`);
      hsprcData = await getBuildingHsprcInfo(buildingCodes, mgmBldrgstPk);
    }

    // 7. 데이터 가공
    const processedData = processMultiUnitBuildingData(
      recapData, titleData, areaData, landCharacteristics, hsprcData, landShare, 동, 호수
    );

    if (Object.keys(processedData).length === 0) {
      logger.warn(`처리된 데이터가 없습니다: ${record.id}`);
      return false;
    }

    // 8. 에어테이블 업데이트
    const updateData = {};
    Object.keys(processedData).forEach(key => {
      const value = processedData[key];
      if (value !== null && value !== undefined && value !== '') {
        updateData[key] = value;
      }
    });

    if (Object.keys(updateData).length === 0) {
      logger.warn(`업데이트할 유효한 데이터가 없음: ${record.id}`);
      return false;
    }

    await airtableBase(MULTI_UNIT_TABLE).update(record.id, updateData);
    logger.info(`✅ 에어테이블 업데이트 성공: ${record.id}`);
    
    return true;
  } catch (error) {
    logger.error(`❌ 레코드 처리 실패 ${record.id}:`, error.message);
    return false;
  }
};

// 메인 작업 실행 함수
const runMultiUnitBuildingJob = async () => {
  try {
    logger.info('🚀 집합건물 정보 수집 작업 시작...');

    const allRecords = await airtableBase(MULTI_UNIT_TABLE)
      .select({
        view: MULTI_UNIT_VIEW
      })
      .all();

    logger.info(`📋 뷰에서 ${allRecords.length}개 레코드 발견`);

    if (allRecords.length === 0) {
      logger.info('✅ 처리할 레코드가 없습니다');
      return { total: 0, success: 0 };
    }

    // 뷰에서 가져온 모든 레코드를 처리
    const recordData = allRecords.map(record => ({
      id: record.id,
      '지번 주소': record.get('지번 주소') || '',
      '동': record.get('동') || '',
      '호수': record.get('호수') || ''
    }));

    // 직렬 처리
    logger.info(`⏳ 직렬 처리 시작 (총 ${recordData.length}개)...`);

    let successCount = 0;
    for (let i = 0; i < recordData.length; i++) {
      const record = recordData[i];
      
      try {
        logger.info(`\n📍 [${i + 1}/${recordData.length}] 처리 중: ${record.id}`);
        const success = await processMultiUnitBuildingRecord(record);
        
        if (success) {
          successCount++;
          logger.info(`✅ [${i + 1}/${recordData.length}] 성공: ${record.id}`);
        } else {
          logger.warn(`❌ [${i + 1}/${recordData.length}] 실패: ${record.id}`);
        }

        // 마지막 레코드가 아니면 대기
        if (i < recordData.length - 1) {
          await delay(API_DELAY);
        }

      } catch (error) {
        logger.error(`❌ 레코드 처리 중 예외 발생 ${record.id}:`, error.message);
      }
    }

    const failedCount = recordData.length - successCount;
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
    const sampleRecords = await airtableBase(MULTI_UNIT_TABLE)
      .select({
        view: MULTI_UNIT_VIEW,
        maxRecords: 3
      })
      .all();

    // 뷰에 레코드가 있으면 작업 실행
    if (sampleRecords.length > 0) {
      logger.info('🎯 처리할 집합건물 레코드 발견, 작업 실행 중...');
      await runMultiUnitBuildingJob();
    } else {
      logger.debug('✅ 처리할 집합건물 레코드 없음, 작업 건너뜀');
    }
  } catch (error) {
    logger.error('❌ 작업 확인 중 오류 발생:', error.message);
  }
});

// 로그 정리 (매일 자정)
cron.schedule('0 0 * * *', () => {
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
});

// Express 설정
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// API 엔드포인트들
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    service: 'multi-unit-building-service',
    timestamp: new Date().toISOString(),
    version: '3.3.0',
    viewId: MULTI_UNIT_VIEW
  });
});

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

app.get('/view-info', async (req, res) => {
  try {
    const allRecords = await airtableBase(MULTI_UNIT_TABLE)
      .select({
        view: MULTI_UNIT_VIEW,
        maxRecords: 20
      })
      .all();

    const recordsInfo = allRecords.map(record => ({
      id: record.id,
      지번주소: record.get('지번 주소'),
      동: record.get('동'),
      호수: record.get('호수')
    }));

    res.json({
      viewId: MULTI_UNIT_VIEW,
      totalRecords: allRecords.length,
      sampleRecords: recordsInfo
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/test-record/:recordId', async (req, res) => {
  try {
    const recordId = req.params.recordId;
    const record = await airtableBase(MULTI_UNIT_TABLE).find(recordId);
    
    const recordData = {
      id: recordId,
      '지번 주소': record.get('지번 주소'),
      '동': record.get('동') || '',
      '호수': record.get('호수')
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

app.get('/api-status', async (req, res) => {
  try {
    const testUrl = 'https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo';
    const testParams = {
      serviceKey: PUBLIC_API_KEY,
      sigunguCd: '11680',
      bjdongCd: '10600',
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

    const isValid = response.data && response.data.response && response.data.response.header && response.data.response.header.resultCode === '00';

    res.json({
      apiKeyValid: isValid,
      responseCode: response.data && response.data.response && response.data.response.header ? response.data.response.header.resultCode : 'unknown',
      responseMessage: response.data && response.data.response && response.data.response.header ? response.data.response.header.resultMsg : 'unknown',
      hasApiKey: !!PUBLIC_API_KEY,
      hasVWorldKey: !!VWORLD_APIKEY
    });
  } catch (error) {
    res.status(500).json({
      apiKeyValid: false,
      error: error.message,
      hasApiKey: !!PUBLIC_API_KEY,
      hasVWorldKey: !!VWORLD_APIKEY
    });
  }
});

app.get('/', (req, res) => {
  res.send(`
    <html>
    <head>
        <title>집합건물 서비스 관리 v3.3</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .button { display: inline-block; padding: 10px 20px; margin: 10px; 
                     background: #007bff; color: white; text-decoration: none; 
                     border-radius: 5px; }
            .button:hover { background: #0056b3; }
            .info { background: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0; }
            .feature { background: #e7f3ff; padding: 15px; border-radius: 5px; margin: 10px 0; }
            .fix { background: #d4edda; padding: 15px; border-radius: 5px; margin: 10px 0; }
        </style>
    </head>
    <body>
        <h1>🏗️ 집합건물 서비스 관리 v3.3</h1>
        
        <div class="info">
            <h3>📋 현재 설정</h3>
            <p><strong>뷰 ID:</strong> ${MULTI_UNIT_VIEW}</p>
            <p><strong>API 지연시간:</strong> ${API_DELAY/1000}초</p>
            <p><strong>스케줄:</strong> 1분마다 실행</p>
        </div>

        <div class="fix">
            <h3>🔧 v3.3 최종 수정사항</h3>
            <ul>
                <li><strong>데이터 타입 최적화:</strong> 면적/비율/수량 필드를 숫자로 처리</li>
                <li><strong>동/호수 매칭 개선:</strong> "102동"↔"102", "1003호"↔"1003" 자동 매칭</li>
                <li><strong>숫자 필드:</strong> 면적, 건폐율, 용적률, 높이, 주차대수, 승강기수, 주택가격</li>
                <li><strong>문자 필드:</strong> 총층수(-0/3), 세대/가구/호, 용도지역, 주소 등</li>
            </ul>
        </div>

        <h3>🔧 관리 기능</h3>
        <a href="/health" class="button">상태 확인</a>
        <a href="/view-info" class="button">뷰 정보 확인</a>
        <a href="/api-status" class="button">API 상태 확인</a>
        <a href="/run-job" class="button">수동 작업 실행</a>

        <h3>📊 모니터링</h3>
        <p>로그 확인: <code>pm2 logs multi-unit-service</code></p>
        <p>프로세스 상태: <code>pm2 list</code></p>
        
        <h3>🚀 서버 재시작 방법</h3>
        <p><code>pm2 stop multi-unit-service</code></p>
        <p><code>pm2 start multi-unit-app.js --name multi-unit-service</code></p>
        
        <h3>📝 처리되는 필드들</h3>
        <div class="info">
            <p><strong>기본 정보:</strong> 도로명주소, 건물명, 높이, 주구조, 지붕, 주용도</p>
            <p><strong>면적 정보:</strong> 대지면적, 연면적, 건축면적, 전용면적, 공급면적, 토지면적</p>
            <p><strong>비율 정보:</strong> 건폐율, 용적률</p>
            <p><strong>세대 정보:</strong> 총 세대/가구/호, 해당동 세대/가구/호</p>
            <p><strong>시설 정보:</strong> 총층수(-지하/지상), 총주차대수, 해당동 승강기수, 주건물수</p>
            <p><strong>기타 정보:</strong> 사용승인일, 용도지역, 주택가격, 대지지분</p>
        </div>
    </body>
    </html>
  `);
});

// 서버 시작
app.listen(PORT, () => {
  logger.info('🚀 집합건물 서비스 v3.3 시작됨');
  logger.info(`📡 포트: ${PORT}`);
  logger.info(`🌐 웹 인터페이스: http://localhost:${PORT}`);
  logger.info(`📋 사용 뷰: ${MULTI_UNIT_VIEW}`);
  logger.info(`⏱️ API 지연시간: ${API_DELAY/1000}초`);
  logger.info(`🔄 스케줄: 1분마다 실행`);
});

module.exports = app;