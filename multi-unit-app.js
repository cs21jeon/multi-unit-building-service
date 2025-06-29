require('dotenv').config({ path: '/root/goldenrabbit/.env' });
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

const getBuildingTitleInfo = async (codeData, dongNm = null) => {
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

const getBuildingJijiguInfo = async (codeData) => {
  try {
    await delay(API_DELAY);
    
    const response = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrJijiguInfo', {
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
    logger.error('getBuildingJijiguInfo 실패:', error.message);
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
    
    if (jsonData?.response?.fields?.field) {
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
  if (!data?.response?.body?.items?.item) return [];
  
  const items = data.response.body.items.item;
  return Array.isArray(items) ? items : [items];
};

const findMgmBldrgstPk = (exposData, dongNm, hoNm) => {
  const items = extractItems(exposData);
  
  for (const item of items) {
    if (dongNm && dongNm.trim()) {
      // 동이 있는 경우: 동과 호수가 일치하고 주건축물+전유인 경우
      if (item.dongNm === dongNm.trim() && 
          item.hoNm === hoNm &&
          item.mainAtchGbCdNm === "주건축물" && 
          item.exposPubuseGbCdNm === "전유") {
        return item.mgmBldrgstPk;
      }
    } else {
      // 동이 없는 경우: 호수만 일치하는 경우
      if (item.hoNm === hoNm && 
          item.mainAtchGbCdNm === "주건축물" && 
          item.exposPubuseGbCdNm === "전유") {
        return item.mgmBldrgstPk;
      }
    }
  }
  
  return null;
};

const processMultiUnitBuildingData = (recapData, titleData, areaData, jijiguData, hsprcData, landShare, dongNm, hoNm) => {
  const result = {};
  
  // 총괄표제부 데이터가 있는지 확인
  const hasRecapData = recapData?.response?.body?.totalCount && parseInt(recapData.response.body.totalCount) > 0;
  
  if (hasRecapData) {
    // === getBrRecapTitleInfo가 있는 경우 ===
    logger.info('총괄표제부 데이터 처리 중 (아파트 등)');
    
    const recapItems = extractItems(recapData);
    if (recapItems.length > 0) {
      const recap = recapItems[0];
      
      // 1. 총괄표제부에서 기본 정보 (실제 필드명 사용)
      result["대지면적(㎡)"] = parseFloat(recap.platArea) || null;
      result["연면적(㎡)"] = parseFloat(recap.totArea) || null;
      result["용적률산정용연면적(㎡)"] = parseFloat(recap.vlRatEstmTotArea) || null;
      result["건축면적(㎡)"] = parseFloat(recap.archArea) || null;
      result["건폐율(%)"] = parseFloat(recap.bcRat) || null;
      result["용적률(%)"] = parseFloat(recap.vlRat) || null;
      result["건물명"] = recap.bldNm || null;
      result["총주차대수"] = parseInt(recap.totPkngCnt) || null;
      result["사용승인일"] = formatDateISO(recap.useAprDay);
      
      const 총세대수 = parseInt(recap.hhldCnt) || 0;
      const 총가구수 = parseInt(recap.fmlyCnt) || 0;
      const 총호수 = parseInt(recap.hoCnt) || 0;
      result["총 세대/가구/호"] = `${총세대수}/${총가구수}/${총호수}`;
      result["주건물수"] = parseInt(recap.mainBldCnt) || null;
    }
    
    // 2. 표제부에서 해당 동 정보
    const titleItems = extractItems(titleData);
    if (titleItems.length > 0) {
      let matchingDong = null;
      
      if (dongNm && dongNm.trim()) {
        matchingDong = titleItems.find(item => item.dongNm && item.dongNm.trim() === dongNm.trim());
      } else {
        matchingDong = titleItems.find(item => item.mainAtchGbCdNm === "주건축물");
      }
      
      if (matchingDong) {
        result["높이(m)"] = parseFloat(matchingDong.heit) || null;
        result["주구조"] = matchingDong.strctCdNm || null;
        result["지붕"] = matchingDong.roofCdNm || null;
        result["주용도"] = matchingDong.mainPurpsCdNm || null;
        result["총층수"] = parseInt(matchingDong.grndFlrCnt) || null; // "해당동 총층수" -> "총층수"
        
        const 해당동세대수 = parseInt(matchingDong.hhldCnt) || 0;
        const 해당동가구수 = parseInt(matchingDong.fmlyCnt) || 0;
        const 해당동호수 = parseInt(matchingDong.hoCnt) || 0;
        result["해당동 세대/가구/호"] = `${해당동세대수}/${해당동가구수}/${해당동호수}`;
        
        const 승강기수 = (parseInt(matchingDong.rideUseElvtCnt) || 0) + (parseInt(matchingDong.emgenUseElvtCnt) || 0);
        result["해당동 승강기수"] = 승강기수 > 0 ? 승강기수 : null;
      }
    }
    
  } else {
    // === getBrRecapTitleInfo가 없는 경우 ===
    logger.info('총괄표제부 없음, 표제부 데이터 처리 중 (빌라, 다세대 등)');
    
    const titleItems = extractItems(titleData);
    if (titleItems.length > 0) {
      const mainInfo = titleItems[0];
      
      // 1. 표제부에서 모든 정보
      result["도로명주소"] = mainInfo.newPlatPlc || null;
      result["높이(m)"] = parseFloat(mainInfo.heit) || null;
      result["주구조"] = mainInfo.strctCdNm || null;
      result["지붕"] = mainInfo.roofCdNm || null;
      result["주용도"] = mainInfo.mainPurpsCdNm || null;
      result["총층수"] = parseInt(mainInfo.grndFlrCnt) || null; // "해당동 총층수" -> "총층수"
      
      const 세대수 = parseInt(mainInfo.hhldCnt) || 0;
      const 가구수 = parseInt(mainInfo.fmlyCnt) || 0;
      const 호수 = parseInt(mainInfo.hoCnt) || 0;
      result["해당동 세대/가구/호"] = `${세대수}/${가구수}/${호수}`;
      
      const 주차대수 = (parseInt(mainInfo.indrMechUtcnt) || 0) + 
                     (parseInt(mainInfo.oudrMechUtcnt) || 0) + 
                     (parseInt(mainInfo.indrAutoUtcnt) || 0) + 
                     (parseInt(mainInfo.oudrAutoUtcnt) || 0);
      result["총주차대수"] = 주차대수 > 0 ? 주차대수 : null;
      
      const 승강기수 = (parseInt(mainInfo.rideUseElvtCnt) || 0) + (parseInt(mainInfo.emgenUseElvtCnt) || 0);
      result["해당동 승강기수"] = 승강기수 > 0 ? 승강기수 : null;
    }
  }
  
  // 3. 면적 정보 (공통)
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
    
    result["전용면적(㎡)"] = 전용면적 > 0 ? 전용면적 : null;
    result["공급면적(㎡)"] = (전용면적 + 공용면적) > 0 ? (전용면적 + 공용면적) : null;
  }
  
  // 4. 지구지역 정보 (공통) - 필드명 수정
  if (jijiguData) {
    const jijiguItems = extractItems(jijiguData);
    if (jijiguItems.length > 0 && jijiguItems[0].jijiguGbCdNm) {
      // "용도지역"이 select 필드인 경우, 기존 옵션과 일치하는지 확인 후 입력
      const 용도지역값 = jijiguItems[0].jijiguGbCdNm;
      if (용도지역값 && 용도지역값.trim() !== '') {
        result["용도지역"] = 용도지역값;
      }
    }
  }
  
  // 5. 주택가격 정보 (공통)
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
        
        result["주택가격(만원)"] = 주택가격만원 > 0 ? 주택가격만원 : 0;
        result["주택가격기준일"] = formatDateISO(latestPrice.crtnDay);
      }
    } else {
      result["주택가격(만원)"] = 0;
    }
  } else {
    result["주택가격(만원)"] = 0;
  }
  
  // 6. 대지지분 정보 (공통)
  if (landShare !== null) {
    result["대지지분(㎡)"] = landShare;
  }
  
  return result;
};

const needsProcessing = (record) => {
  try {
    const 지번주소 = record.get('지번 주소');
    const 호수 = record.get('호수');

    if (!지번주소 || !호수) {
      return false;
    }

    // 실제 Airtable 필드명에 맞게 수정
    const 검사필드목록 = [
      '도로명주소', 
      '전용면적(㎡)', 
      '연면적(㎡)', 
      '주구조', 
      '주용도',
      '총층수',
      '해당동 승강기수', 
      '총 세대/가구/호', 
      '총주차대수',
      '주택가격(만원)', 
      '사용승인일', 
      '대지지분(㎡)',
      '용도지역'
    ];

    for (const 필드 of 검사필드목록) {
      const 값 = record.get(필드);
      if (!값 || 값 === '' || 값 === null || 값 === undefined) {
        return true;
      }
    }

    return false;
  } catch (error) {
    logger.error(`레코드 처리 필요성 검사 중 오류:`, error);
    return false;
  }
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
    
    // 3. PNU 생성 (대지지분 조회용)
    const pnu = generatePNU(buildingCodes);

    // 4. API 데이터 수집
    logger.info(`📡 API 데이터 수집 시작...`);
    
    const [recapData, titleData, areaData, jijiguData, exposData, landShare] = await Promise.all([
      getBuildingRecapInfo(buildingCodes),
      getBuildingTitleInfo(buildingCodes, 동),
      getBuildingAreaInfo(buildingCodes, 동, 호수),
      getBuildingJijiguInfo(buildingCodes),
      getBuildingExposInfo(buildingCodes, 동, 호수),
      pnu ? getLandShareInfo(pnu, 동, 호수) : Promise.resolve(null)
    ]);

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
      recapData, titleData, areaData, jijiguData, hsprcData, landShare, 동, 호수
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

    // 처리가 필요한 레코드만 필터링
    const recordsToProcess = allRecords.filter(record => needsProcessing(record));

    logger.info(`🎯 처리 대상 레코드: ${recordsToProcess.length}개`);

    if (recordsToProcess.length === 0) {
      logger.info('✅ 처리할 레코드가 없습니다');
      return { total: 0, success: 0 };
    }

    // 레코드 정보 추출
    const recordData = recordsToProcess.map(record => ({
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
    version: '3.0.0',
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
      호수: record.get('호수'),
      needsProcessing: needsProcessing(record)
    }));

    const needsProcessingCount = recordsInfo.filter(r => r.needsProcessing).length;

    res.json({
      viewId: MULTI_UNIT_VIEW,
      totalRecords: allRecords.length,
      needsProcessing: needsProcessingCount,
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

    const isValid = response.data?.response?.header?.resultCode === '00';

    res.json({
      apiKeyValid: isValid,
      responseCode: response.data?.response?.header?.resultCode,
      responseMessage: response.data?.response?.header?.resultMsg,
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
        <title>집합건물 서비스 관리 v3.0</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            .button { display: inline-block; padding: 10px 20px; margin: 10px; 
                     background: #007bff; color: white; text-decoration: none; 
                     border-radius: 5px; }
            .button:hover { background: #0056b3; }
            .info { background: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0; }
            .feature { background: #e7f3ff; padding: 15px; border-radius: 5px; margin: 10px 0; }
        </style>
    </head>
    <body>
        <h1>🏗️ 집합건물 서비스 관리 v3.0</h1>
        
        <div class="info">
            <h3>📋 현재 설정</h3>
            <p><strong>뷰 ID:</strong> ${MULTI_UNIT_VIEW}</p>
            <p><strong>API 지연시간:</strong> ${API_DELAY/1000}초</p>
            <p><strong>스케줄:</strong> 1분마다 실행</p>
        </div>

        <div class="feature">
            <h3>🆕 v3.0 주요 개선사항</h3>
            <ul>
                <li><strong>총괄표제부 분기 처리:</strong> 아파트와 빌라/다세대 구분</li>
                <li><strong>새로운 API 추가:</strong> getBrExposInfo, VWorld ldaregList</li>
                <li><strong>mgmBldrgstPk 자동 추출:</strong> 동/호수 기반 매칭</li>
                <li><strong>대지지분 정보:</strong> VWorld API 연동</li>
                <li><strong>코드 최적화:</strong> 불필요한 부분 제거, 에러 처리 개선</li>
            </ul>
        </div>

        <h3>🔧 관리 기능</h3>
        <a href="/health" class="button">상태 확인</a>
        <a href="/view-info" class="button">뷰 정보 확인</a>
        <a href="/api-status" class="button">API 상태 확인</a>
        <a href="/run-job" class="button">수동 작업 실행</a>

        <h3>📊 모니터링</h3>
        <p>로그 확인: <code>pm2 logs multi-unit-building-service</code></p>
        <p>프로세스 상태: <code>pm2 status</code></p>
        
        <h3>📝 처리 필드</h3>
        <div class="info">
            <p><strong>총괄표제부 있는 경우 (아파트 등):</strong></p>
            <ul>
                <li>총괄표제부: 대지면적, 연면적, 건축면적, 건폐율, 용적률, 건물명, 총주차대수, 사용승인일, 총 세대/가구/호, 주건물수</li>
                <li>표제부(해당동): 높이, 주구조, 지붕, 주용도, 해당동 총층수, 해당동 세대/가구/호, 해당동 승강기수</li>
                <li>공통: 전용면적, 공급면적, 용도지역, 주택가격, 대지지분</li>
            </ul>
            
            <p><strong>총괄표제부 없는 경우 (빌라, 다세대 등):</strong></p>
            <ul>
                <li>표제부: 도로명주소, 높이, 주구조, 지붕, 주용도, 해당동 총층수, 해당동 세대/가구/호, 총주차대수, 해당동 승강기수</li>
                <li>공통: 전용면적, 공급면적, 용도지역, 주택가격, 대지지분</li>
            </ul>
        </div>
    </body>
    </html>
  `);
});

// 서버 시작
app.listen(PORT, () => {
  logger.info('🚀 집합건물 서비스 v3.0 시작됨');
  logger.info(`📡 포트: ${PORT}`);
  logger.info(`🌐 웹 인터페이스: http://localhost:${PORT}`);
  logger.info(`📋 사용 뷰: ${MULTI_UNIT_VIEW}`);
  logger.info(`⏱️ API 지연시간: ${API_DELAY/1000}초`);
  logger.info(`🔄 스케줄: 1분마다 실행`);
});

module.exports = app;