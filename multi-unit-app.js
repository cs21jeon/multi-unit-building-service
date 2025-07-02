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

const API_DELAY = 800; // 2000 → 800ms로 단축 (병렬 처리로 보완)
const MAX_RETRIES = 2;
const RETRY_DELAY = 3000;

// 유틸리티 함수들
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// 문자열에서 숫자만 추출하는 유틸리티 함수 추가 (
const extractNumbersOnly = (str) => {
  if (!str || typeof str !== 'string') return '';
  return str.replace(/[^0-9]/g, '');
};

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
  
  // YYYYMMDD를 YYYY-MM-DD로 변환
  const year = dateStr.substring(0, 4);
  const month = dateStr.substring(4, 6);
  const day = dateStr.substring(6, 8);
  const formattedDate = `${year}-${month}-${day}`;
  
  // 유효한 날짜인지 검증
  const date = new Date(`${formattedDate}T00:00:00.000Z`);
  if (isNaN(date.getTime())) {
    logger.warn(`잘못된 날짜 형식: ${dateStr}`);
    return null;
  }
  
  return date.toISOString();
};

// 호수에서 실제 호수 번호만 추출하는 함수 (새로 추가)
const extractHoNumber = (hoStr) => {
  if (!hoStr || typeof hoStr !== 'string') return '';
  
  // "1층201호", "지하1층B102호" 등에서 호수만 추출
  const hoMatch = hoStr.match(/(\d+)호$/);
  if (hoMatch) {
    return hoMatch[1]; // 마지막 숫자호수만 반환 (201호 → 201)
  }
  
  // "호"가 없는 경우 기존 로직 사용
  return extractNumbersOnly(hoStr);
};

// 동/호수 처리 유틸리티 함수 수정
const processDongHo = (dongNm, hoNm) => {
  // 1. 동 처리 (기존과 동일)
  let dongVariations = [];
  if (dongNm && dongNm.trim() !== '') {
    // 원본 값 추가
    dongVariations.push(dongNm.trim());
    
    // 숫자만 추출한 값 추가 (없으면 빈 문자열)
    const dongNumbers = extractNumbersOnly(dongNm);
    if (dongNumbers !== dongNm.trim()) {
      dongVariations.push(dongNumbers);
    }
    
    // "동" 접미사 제거한 값 추가
    const dongWithoutSuffix = dongNm.trim().replace(/동$/, '');
    if (dongWithoutSuffix !== dongNm.trim() && dongWithoutSuffix !== dongNumbers) {
      dongVariations.push(dongWithoutSuffix);
    }
  } else {
    dongVariations.push(''); // 빈 값도 시도
  }
  
  // 2. 호수 처리 (수정됨)
  let hoVariations = [];
  if (hoNm && hoNm.trim() !== '') {
    // 원본 값 추가
    hoVariations.push(hoNm.trim());
    
    // 실제 호수 번호만 추출한 값 추가 (1층201호 → 201)
    const hoNumber = extractHoNumber(hoNm);
    if (hoNumber !== hoNm.trim() && hoNumber !== '') {
      hoVariations.push(hoNumber);
    }
    
    // "호" 접미사 제거한 값 추가
    const hoWithoutSuffix = hoNm.trim().replace(/호$/, '');
    if (hoWithoutSuffix !== hoNm.trim() && hoWithoutSuffix !== hoNumber) {
      hoVariations.push(hoWithoutSuffix);
    }
  } else {
    hoVariations.push(''); // 빈 값도 시도
  }
  
  return { dongVariations, hoVariations };
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

// 면적 정보 조회 함수 수정
const getBuildingAreaInfo = async (codeData, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    // 1단계: 원본 동/호수로 조회
    logger.info(`🏢 면적 정보 조회 시작 - 원본 동/호: 동='${dongNm || ""}', 호='${hoNm || ""}'`);
    
    const originalResponse = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo', {
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
    
    const originalTotalCount = originalResponse.data?.response?.body?.totalCount || 0;
    logger.info(`1단계 면적 정보 조회 결과: totalCount=${originalTotalCount}`);
    
    if (originalTotalCount > 0) {
      logger.info(`✅ 1단계 면적 정보 조회 성공 - 원본 동/호 사용`);
      return originalResponse.data;
    }
    
    // 2단계: 숫자만 추출하여 조회
    if (dongNm || hoNm) {
      const numericDong = extractNumbersOnly(dongNm || '');
      const numericHo = extractNumbersOnly(hoNm || '');
      
      logger.info(`🔄 2단계 면적 정보 조회 시도 - 숫자만 추출: 동='${numericDong}', 호='${numericHo}'`);
      
      // 숫자 값이 있을 때만 시도
      if (numericDong || numericHo) {
        const numericResponse = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo', {
          params: {
            serviceKey: PUBLIC_API_KEY,
            sigunguCd: codeData.시군구코드,
            bjdongCd: codeData.법정동코드,
            bun: codeData.번,
            ji: codeData.지,
            dongNm: numericDong,
            hoNm: numericHo,
            _type: 'json',
            numOfRows: 50,
            pageNo: 1
          },
          timeout: 30000
        });
        
        const numericTotalCount = numericResponse.data?.response?.body?.totalCount || 0;
        logger.info(`2단계 면적 정보 조회 결과: totalCount=${numericTotalCount}`);
        
        if (numericTotalCount > 0) {
          logger.info(`✅ 2단계 면적 정보 조회 성공 - 숫자만 추출 사용`);
          return numericResponse.data;
        }
      }
    }
    
    // 3단계: 동/호 파라미터 없이 조회 (마지막 시도)
    logger.info(`🔄 3단계 면적 정보 조회 시도 - 동/호 파라미터 없이`);
    
    const fallbackResponse = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo', {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        _type: 'json',
        numOfRows: 100,  // 더 많은 결과를 가져옴
        pageNo: 1
      },
      timeout: 30000
    });
    
    const fallbackTotalCount = fallbackResponse.data?.response?.body?.totalCount || 0;
    logger.info(`3단계 면적 정보 조회 결과: totalCount=${fallbackTotalCount}`);
    
    if (fallbackTotalCount > 0) {
      // 결과가 있으면, 후처리로 동/호수 필터링 시도
      const allItems = extractItems(fallbackResponse.data);
      logger.info(`전체 면적 정보 항목 수: ${allItems.length}`);
      
      // 로깅을 위해 처음 몇 개 항목만 출력
      const sampleSize = Math.min(allItems.length, 5);
      logger.info(`면적 정보 샘플 (${sampleSize}개):`);
      for (let i = 0; i < sampleSize; i++) {
        const item = allItems[i];
        logger.info(`- 항목 ${i+1}: 동=${item.dongNm || '없음'}, 호=${item.hoNm || '없음'}, 면적=${item.area || '0'}`);
      }
      
      // 동/호로 필터링 시도 (여러 변형 사용)
      let matchedItems = [];
      
      // 호수로 매칭 시도 (우선순위)
      if (hoNm) {
        const hoVariations = [
          hoNm,
          hoNm.replace(/호$/, ''),
          extractNumbersOnly(hoNm),
          hoNm.replace(/층/, '')
        ].filter(v => v); // 빈 값 제거
        
        for (const hoVar of hoVariations) {
          const matched = allItems.filter(item => 
            item.hoNm === hoVar || 
            extractNumbersOnly(item.hoNm || '') === extractNumbersOnly(hoVar)
          );
          
          if (matched.length > 0) {
            matchedItems = matched;
            logger.info(`✅ 호수 '${hoVar}'로 ${matched.length}개 항목 매칭 성공`);
            break;
          }
        }
      }
      
      // 호수로 매칭 실패하고 동이 있으면 동으로 매칭 시도
      if (matchedItems.length === 0 && dongNm) {
        const dongVariations = [
          dongNm,
          dongNm.replace(/동$/, ''),
          extractNumbersOnly(dongNm)
        ].filter(v => v); // 빈 값 제거
        
        for (const dongVar of dongVariations) {
          const matched = allItems.filter(item => 
            item.dongNm === dongVar || 
            extractNumbersOnly(item.dongNm || '') === extractNumbersOnly(dongVar)
          );
          
          if (matched.length > 0) {
            matchedItems = matched;
            logger.info(`✅ 동 '${dongVar}'로 ${matched.length}개 항목 매칭 성공`);
            break;
          }
        }
      }
      
      if (matchedItems.length > 0) {
        // 매칭된 항목만 포함한 응답 구성
        const filteredResponse = {...fallbackResponse.data};
        if (filteredResponse.response && filteredResponse.response.body) {
          filteredResponse.response.body.items.item = matchedItems.length === 1 ? matchedItems[0] : matchedItems;
          filteredResponse.response.body.totalCount = String(matchedItems.length);
          logger.info(`✅ 3단계 면적 정보 조회 성공 - 후처리 필터링 사용`);
          return filteredResponse;
        }
      }
      
      // 필터링 실패했지만 전체 데이터는 있는 경우
      logger.warn(`⚠️ 동/호 필터링 실패했지만 전체 데이터 반환 (${fallbackTotalCount}개 항목)`);
      return fallbackResponse.data;
    }
    
    // 모든 시도 실패 시 null 반환 (중요: 0이 아닌 null 반환)
    logger.warn(`❌ 모든 단계에서 면적 정보 조회 실패`);
    return null;
  } catch (error) {
    logger.error('getBuildingAreaInfo 실패:', error.message);
    if (error.response) {
      logger.error('API 응답 상태:', error.response.status);
      logger.error('API 응답 데이터:', JSON.stringify(error.response.data, null, 2).substring(0, 500) + '...');
    }
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

// VWorld API를 사용한 토지특성 정보 조회 (용도지역, 토지면적) - 디버깅 강화
const getLandCharacteristics = async (pnu) => {
  try {
    logger.info(`🌍 VWorld 토지특성 정보 조회 시작 - PNU: ${pnu}`);
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

    logger.debug(`VWorld 토지특성 응답 상태: ${response.status}`);
    logger.debug(`VWorld 토지특성 응답 크기: ${response.data ? response.data.length : 0} bytes`);

    const jsonData = convert.xml2js(response.data, { compact: true, spaces: 2, textKey: '_text' });
    
    logger.debug(`VWorld 토지특성 변환된 JSON:`, JSON.stringify(jsonData, null, 2));
    
    if (jsonData && jsonData.response && jsonData.response.fields && jsonData.response.fields.field) {
      let fields = jsonData.response.fields.field;
      if (!Array.isArray(fields)) fields = [fields];
      
      if (fields.length > 0) {
        const field = fields[0];
        
        const result = {
          용도지역: field.prposArea1Nm && field.prposArea1Nm._text ? field.prposArea1Nm._text : null,
          토지면적: field.lndpclAr && field.lndpclAr._text ? parseFloat(field.lndpclAr._text) : null
        };
        
        logger.info(`✅ VWorld 토지특성 성공 - 용도지역: ${result.용도지역}, 토지면적: ${result.토지면적}`);
        return result;
      } else {
        logger.warn(`⚠️ VWorld 토지특성 - fields 배열이 비어있음`);
      }
    } else {
      logger.warn(`⚠️ VWorld 토지특성 - 응답 구조 이상: response.fields.field가 없음`);
      if (jsonData && jsonData.response && jsonData.response.header) {
        logger.warn(`VWorld 응답 헤더:`, JSON.stringify(jsonData.response.header, null, 2));
      }
    }
    
    return { 용도지역: null, 토지면적: null };
  } catch (error) {
    logger.error(`❌ VWorld 토지특성 조회 실패 (PNU: ${pnu}):`, error.message);
    if (error.response) {
      logger.error(`VWorld API 응답 상태: ${error.response.status}`);
      logger.error(`VWorld API 응답 데이터:`, error.response.data);
    }
    return { 용도지역: null, 토지면적: null };
  }
};

// 호수 매칭 함수 개선 (유연한 매칭)
const isHoMatch = (apiHo, inputHo) => {
  if (!inputHo || !apiHo) return false;
  
  const apiHoStr = String(apiHo).trim();
  const inputHoStr = String(inputHo).trim();
  
  // 1. 완전 일치 (우선순위 최고)
  if (apiHoStr === inputHoStr) {
    return true;
  }
  
  // 2. 호수 부분만 추출해서 비교
  const getHoNumber = (hoStr) => {
    // "1층201호" → "201", "201호" → "201", "201" → "201" 
    const match = hoStr.match(/(\d+)호?$/);
    return match ? match[1] : hoStr.replace(/[^0-9]/g, '');
  };
  
  const apiNumber = getHoNumber(apiHoStr);
  const inputNumber = getHoNumber(inputHoStr);
  
  // 3. 숫자 부분이 일치하면 매칭 (201호 ↔ 201, 1층201호 ↔ 201)
  if (apiNumber && inputNumber && apiNumber === inputNumber) {
    return true;
  }
  
  return false;
};

// 주택가격 조회 함수 수정
const getHousingPriceInfo = async (pnu, dongNm, hoNm) => {
  try {
    logger.info(`🏠 VWorld 주택가격 정보 조회 시작 - PNU: ${pnu}, 동: ${dongNm}, 호: ${hoNm}`);
    
    // 동/호수 변형 생성
    const { dongVariations, hoVariations } = processDongHo(dongNm, hoNm);
    
    // 여러 변형을 순차적으로 시도
    for (const processDong of dongVariations) {
      for (const processHo of hoVariations) {
        logger.info(`주택가격 시도: 동='${processDong}', 호='${processHo}'`);
        
        // 현재 조합으로 시도
        const result = await tryGetHousingPrice(pnu, processDong, processHo);
        if (result.주택가격만원 > 0) {
          logger.info(`✅ 주택가격 성공: 동='${processDong}', 호='${processHo}', 가격=${result.주택가격만원}만원, 년도=${result.주택가격기준년도}`);
          return result;
        }
      }
    }
    
    // 모든 시도 실패 시
    logger.warn(`❌ 모든 동/호 변형으로 주택가격 조회 실패`);
    return {
      주택가격만원: 0,
      주택가격기준년도: 0
    };
  } catch (error) {
    logger.error(`❌ VWorld 주택가격 조회 실패 (PNU: ${pnu}):`, error.message);
    return {
      주택가격만원: 0,
      주택가격기준년도: 0
    };
  }
};

// 주택가격 단일 시도 함수
const tryGetHousingPrice = async (pnu, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    // API 파라미터 구성
    const params = {
      key: VWORLD_APIKEY,
      pnu: pnu,
      format: 'json',
      numOfRows: 30,
      pageNo: 1
    };
    
    // 동이름이 있을 때만 파라미터에 추가
    if (dongNm && dongNm.trim() !== '') {
      params.dongNm = dongNm.trim();
    }
    
    // 호수가 있을 때만 파라미터에 추가
    if (hoNm && hoNm.trim() !== '') {
      params.hoNm = hoNm.trim();
    }
    
    const response = await axios.get('https://api.vworld.kr/ned/data/getApartHousingPriceAttr', {
      params: params,
      timeout: 30000
    });

    const apiUrl = 'https://api.vworld.kr/ned/data/getApartHousingPriceAttr?' + new URLSearchParams(params).toString();
    logger.debug(`🌐 주택가격 호출 URL: ${apiUrl}`);
    
    // 데이터가 있는지 확인
    const totalCount = response.data?.apartHousingPrices?.totalCount || 0;
    if (totalCount === 0 || totalCount === "0") {
      logger.debug(`주택가격 데이터 없음 (totalCount: ${totalCount})`);
      return {
        주택가격만원: 0,
        주택가격기준년도: 0
      };
    }
    
    // 응답 처리 (간소화된 버전)
    let items = [];
    
    // apartHousingPrices.field 체크
    if (response.data?.apartHousingPrices?.field) {
      const rawItems = response.data.apartHousingPrices.field;
      items = Array.isArray(rawItems) ? rawItems : [rawItems];
    }
    
    if (items.length > 0) {
      // 가장 최근 데이터를 찾기 위해 stdrYear(연도)로 정렬
      items.sort((a, b) => {
        const yearA = parseInt(a.stdrYear || '0');
        const yearB = parseInt(b.stdrYear || '0');
        return yearB - yearA; // 내림차순 정렬 (최신순)
      });
      
      // 가장 최근 데이터 사용
      const latestItem = items[0];
      const pblntfPc = latestItem.pblntfPc || '';
      const stdrYear = latestItem.stdrYear || '';
      
      // 주택가격 값 파싱 (만원 단위로 변환)
      let priceValue = parseInt(pblntfPc) || 0;
      
      // API 응답이 원 단위라면 만원 단위로 변환
      if (priceValue > 1000000) {
        priceValue = Math.round(priceValue / 10000);
        logger.debug(`주택가격 단위 변환: ${pblntfPc}원 -> ${priceValue}만원`);
      }
      
      // 가격이 있고 연도가 있으면 결과 반환
      if (priceValue > 0 && stdrYear) {
        const yearValue = parseInt(stdrYear) || 0;
        
        return {
          주택가격만원: priceValue,
          주택가격기준년도: yearValue
        };
      }
    }
    
    return {
      주택가격만원: 0,
      주택가격기준년도: 0
    };
  } catch (error) {
    logger.error(`주택가격 시도 중 오류:`, error.message);
    return {
      주택가격만원: 0,
      주택가격기준년도: 0
    };
  }
};

// 대지지분 조회 함수 수정
const getLandShareInfo = async (pnu, dongNm, hoNm) => {
  try {
    logger.info(`🌍 VWorld 대지지분 정보 조회 시작 - PNU: ${pnu}, 동: ${dongNm}, 호: ${hoNm}`);
    
    // 동/호수 변형 생성
    const { dongVariations, hoVariations } = processDongHo(dongNm, hoNm);
    
    // 동이름이 없는 경우를 위해 "0000" 추가
    if (!dongNm || dongNm.trim() === '') {
      dongVariations.push("0000");
    }
    
    // 여러 변형을 순차적으로 시도
    for (const processDong of dongVariations) {
      for (const processHo of hoVariations) {
        logger.info(`대지지분 시도: 동='${processDong}', 호='${processHo}'`);
        
        // 현재 조합으로 시도
        const result = await tryGetLandShare(pnu, processDong, processHo);
        if (result !== null) {
          logger.info(`✅ 대지지분 성공: 동='${processDong}', 호='${processHo}', 지분=${result}`);
          return result;
        }
      }
    }
    
    // 동 파라미터 없이 다시 시도 (최후의 시도)
    logger.info(`🔄 동 파라미터 없이 대지지분 재시도...`);
    const resultWithoutDong = await tryGetLandShare(pnu, '', hoVariations[0]);
    if (resultWithoutDong !== null) {
      logger.info(`✅ 대지지분 성공 (동 파라미터 없이): 호='${hoVariations[0]}', 지분=${resultWithoutDong}`);
      return resultWithoutDong;
    }
    
    // 모든 시도 실패 시
    logger.warn(`❌ 모든 동/호 변형으로 대지지분 조회 실패`);
    return null;
  } catch (error) {
    logger.error(`❌ VWorld 대지지분 조회 실패 (PNU: ${pnu}):`, error.message);
    return null;
  }
};

// 대지지분 단일 시도 함수 수정
const tryGetLandShare = async (pnu, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    // API 파라미터 구성
    const params = {
      key: VWORLD_APIKEY,
      pnu: pnu,
      format: 'json',
      numOfRows: 10,
      pageNo: 1
    };
    
    // 동이름이 있을 때만 파라미터에 추가
    if (dongNm && dongNm.trim() !== '') {
      params.buldDongNm = dongNm.trim();
    }
    
    // 호수가 있을 때만 파라미터에 추가
    if (hoNm && hoNm.trim() !== '') {
      params.buldHoNm = hoNm.trim();
    }
    
    const response = await axios.get('https://api.vworld.kr/ned/data/buldRlnmList', {
      params: params,
      timeout: 30000
    });

    const apiUrl = 'https://api.vworld.kr/ned/data/buldRlnmList?' + new URLSearchParams(params).toString();
    logger.debug(`🌐 대지지분 호출 URL: ${apiUrl}`);
    
    // 데이터가 있는지 확인
    const totalCount = response.data?.ldaregVOList?.totalCount || response.data?.buldRlnmVOList?.totalCount || 0;
    if (totalCount === 0 || totalCount === "0") {
      logger.debug(`대지지분 데이터 없음 (totalCount: ${totalCount})`);
      return null;
    }
    
    // 응답 처리 (간소화된 버전)
    let items = [];
    
    // ldaregVOList 체크
    if (response.data?.ldaregVOList?.ldaregVOList) {
      const rawItems = response.data.ldaregVOList.ldaregVOList;
      items = Array.isArray(rawItems) ? rawItems : [rawItems];
    } 
    // buldRlnmVOList 체크
    else if (response.data?.buldRlnmVOList?.buldRlnmVOList) {
      const rawItems = response.data.buldRlnmVOList.buldRlnmVOList;
      items = Array.isArray(rawItems) ? rawItems : [rawItems];
    }
    
    if (items.length > 0) {
      // 지분 정보 추출
      for (const item of items) {
        const ldaQotaRate = item.ldaQotaRate || item.landShareRate || '';
        if (ldaQotaRate && ldaQotaRate.trim() !== '') {
          const shareValue = parseFloat(ldaQotaRate.split('/')[0]);
          if (!isNaN(shareValue)) {
            return shareValue;
          }
        }
      }
    }
    
    return null;
  } catch (error) {
    logger.error(`대지지분 시도 중 오류:`, error.message);
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

// 호수 정규화 함수 개선 (층 정보 제거)
const normalizeHosu = (value) => {
  if (!value || typeof value !== 'string') return '';
  
  // "1층201호", "지하1층B102호" 등에서 호수만 추출
  const hoMatch = value.match(/(\d+)호$/);
  if (hoMatch) {
    return hoMatch[1]; // 마지막 숫자호수만 반환 (201호 → 201)
  }
  
  // "B102", "1001" 등 기존 로직
  const numbers = value.replace(/[^0-9]/g, '');
  return numbers;
};

const processMultiUnitBuildingData = (recapData, titleData, areaData, landCharacteristics, housingPrice, landShare, dongNm, hoNm) => {
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
      // 사용승인일 처리 - ISO 형식으로 변환
      const 사용승인일 = formatDateISO(recap.useAprDay);
      if (사용승인일) result["사용승인일"] = 사용승인일;
      
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
        
        // 해당 동의 사용승인일 (총괄표제부보다 우선)
        if (matchingDong.useAprDay) {
          const 동사용승인일 = formatDateISO(matchingDong.useAprDay);
          if (동사용승인일) result["사용승인일"] = 동사용승인일;
        }
      }
    }
    
    // 총괄표제부가 있는 경우에도 도로명주소와 건물명은 표제부에서 가져올 수 있음
    if (hasRecapData) {
      const titleItems = extractItems(titleData);
      if (titleItems.length > 0) {
        const firstTitle = titleItems[0];
        if (firstTitle.newPlatPlc && !result["도로명주소"]) {
          result["도로명주소"] = firstTitle.newPlatPlc;
        }
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
      if (mainInfo.bldNm) result["건물명"] = mainInfo.bldNm;
      if (mainInfo.heit) result["높이(m)"] = parseFloat(mainInfo.heit);
      if (mainInfo.strctCdNm) result["주구조"] = mainInfo.strctCdNm;
      if (mainInfo.roofCdNm) result["지붕"] = mainInfo.roofCdNm;
      if (mainInfo.mainPurpsCdNm) result["주용도"] = mainInfo.mainPurpsCdNm;
      
      // 총괄표제부가 없는 경우 표제부 정보를 총괄 필드에도 매핑
      if (mainInfo.platArea) result["대지면적(㎡)"] = parseFloat(mainInfo.platArea);
      if (mainInfo.totArea) result["연면적(㎡)"] = parseFloat(mainInfo.totArea);
      if (mainInfo.vlRatEstmTotArea) result["용적률산정용연면적(㎡)"] = parseFloat(mainInfo.vlRatEstmTotArea);
      if (mainInfo.archArea) result["건축면적(㎡)"] = parseFloat(mainInfo.archArea);
      if (mainInfo.bcRat) result["건폐율(%)"] = parseFloat(mainInfo.bcRat);
      if (mainInfo.vlRat) result["용적률(%)"] = parseFloat(mainInfo.vlRat);
      if (mainInfo.useAprDay) {
        const 사용승인일 = formatDateISO(mainInfo.useAprDay);
        if (사용승인일) result["사용승인일"] = 사용승인일;
      }
      
      // 총층수를 -지하층수/지상층수 형태로 변환
      const 지상층수 = mainInfo.grndFlrCnt || '0';
      const 지하층수 = mainInfo.ugrndFlrCnt || '0';
      result["총층수"] = `-${지하층수}/${지상층수}`;
      
      const 세대수 = mainInfo.hhldCnt || '0';
      const 가구수 = mainInfo.fmlyCnt || '0';
      const 호수 = mainInfo.hoCnt || '0';
      result["해당동 세대/가구/호"] = `${세대수}/${가구수}/${호수}`;
      result["총 세대/가구/호"] = `${세대수}/${가구수}/${호수}`; // 총괄 정보도 동일하게 설정
      
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
      
      // 빌라/다세대는 보통 주건물수가 1개
      result["주건물수"] = 1;
    }
  }
  
  // 3. 면적 정보 (공통) - 항상 포함
  let 전용면적 = null;  // null로 초기화
  let 공용면적 = 0;
  let 공급면적 = null;  // null로 초기화

  if (areaData) {
    const areaItems = extractItems(areaData);
    logger.info(`📏 면적 정보 항목 수: ${areaItems.length}`);
    
    if (areaItems.length > 0) {
      // 항목 정보 로깅
      areaItems.forEach((item, idx) => {
        logger.debug(`면적 항목 ${idx+1}: 유형=${item.exposPubuseGbCdNm || '없음'}, 면적=${item.area || '0'}`);
      });
      
      let tempArea전용 = 0;
      let tempArea공용 = 0;
      
      areaItems.forEach(item => {
        const area = parseFloat(item.area) || 0;
        if (item.mainAtchGbCdNm === "주건축물" && item.exposPubuseGbCdNm === "전유") {
          tempArea전용 += area;
          logger.info(`전용면적 추가: +${area}㎡ (총 ${tempArea전용}㎡)`);
        } else if (item.mainAtchGbCdNm === "주건축물" && item.exposPubuseGbCdNm === "공용") {
          tempArea공용 += area;
          logger.info(`공용면적 추가: +${area}㎡ (총 ${tempArea공용}㎡)`);
        }
      });
      
      // 실제 값이 있을 때만 설정
      if (tempArea전용 > 0) {
        전용면적 = tempArea전용;
      }
      
      공용면적 = tempArea공용; // 공용면적은 에어테이블에 저장하지 않음
      
      // 전용면적이 있을 때만 공급면적 계산
      if (전용면적 !== null) {
        공급면적 = 전용면적 + 공용면적;
      }
    } else {
      logger.warn(`⚠️ 면적 정보 항목이 없습니다`);
    }
  }

  // 면적 정보가 있을 때만 결과에 포함
  if (전용면적 !== null) {
    result["전용면적(㎡)"] = 전용면적;
  }

  if (공급면적 !== null) {
    result["공급면적(㎡)"] = 공급면적;
  }

  logger.info(`📊 최종 면적 정보: 전용=${전용면적 !== null ? 전용면적 : '없음'}㎡, 공용=${공용면적}㎡, 공급=${공급면적 !== null ? 공급면적 : '없음'}㎡`);
  
  // 4. VWorld 토지특성 정보 (용도지역, 토지면적)
  if (landCharacteristics) {
    if (landCharacteristics.용도지역) {
      result["용도지역"] = landCharacteristics.용도지역;
    }
    if (landCharacteristics.토지면적) {
      result["토지면적(㎡)"] = landCharacteristics.토지면적; // 숫자로 처리
    }
  }
  
  // 5. VWorld 주택가격 정보 (수정: 주택가격기준년도로 변경)
  if (housingPrice) {
    if (housingPrice.주택가격만원 !== undefined) {
      result["주택가격(만원)"] = housingPrice.주택가격만원; // 숫자로 처리
    } else {
      result["주택가격(만원)"] = 0;
    }
    
    if (housingPrice.주택가격기준년도 !== undefined) {
      result["주택가격기준년도"] = housingPrice.주택가격기준년도; // 숫자 타입
    } else {
      result["주택가격기준년도"] = 0;
    }
  } else {
    result["주택가격(만원)"] = 0;
    result["주택가격기준년도"] = 0;
  }
  
  // 6. 대지지분 정보 (공통) - 숫자로 처리
  if (landShare !== null) {
    result["대지지분(㎡)"] = landShare; // 이미 parseFloat로 처리된 숫자
  } else {
    result["대지지분(㎡)"] = 0; // 없으면 0으로 설정
  }
  
  return result;
};

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
    logger.info(`📍 생성된 PNU: ${pnu}`);

    // 4. API 데이터 수집 - 병렬 처리
    logger.info(`📡 API 데이터 수집 시작 (병렬 처리)...`);
    
    const startTime = Date.now();
    
    // 모든 API를 병렬로 동시 호출
    const [recapData, titleData, areaData, exposData, landCharacteristics, landShare, housingPrice] = await Promise.all([
      getBuildingRecapInfo(buildingCodes),
      getBuildingTitleInfo(buildingCodes),
      getBuildingAreaInfo(buildingCodes, 동, 호수),
      getBuildingExposInfo(buildingCodes, 동, 호수),
      pnu ? getLandCharacteristics(pnu) : Promise.resolve({ 용도지역: null, 토지면적: null }),
      pnu ? getLandShareInfo(pnu, 동, 호수) : Promise.resolve(null),
      pnu ? getHousingPriceInfo(pnu, 동, 호수) : Promise.resolve({ 
        주택가격만원: 0, 
        주택가격기준년도: 0
      })
    ]);
    
    const apiTime = Date.now() - startTime;
    logger.info(`⚡ API 데이터 수집 완료 (${apiTime}ms)`);

    // 5. 데이터 가공 - housingPrice 올바르게 전달
    const processedData = processMultiUnitBuildingData(
      recapData, titleData, areaData, landCharacteristics, housingPrice, landShare, 동, 호수
    );

    if (Object.keys(processedData).length === 0) {
      logger.warn(`처리된 데이터가 없습니다: ${record.id}`);
      return false;
    }

    // 6. 에어테이블 업데이트
    const updateData = {};
    Object.keys(processedData).forEach(key => {
      // 숫자 0과 빈 문자열도 유효한 값으로 처리
      const value = processedData[key];
      if (value !== null && value !== undefined) {
        updateData[key] = value;
      }
    });

    // 필수 필드 확인 (전용면적과 공급면적은 예외 처리)
    const requiredFields = ["주택가격(만원)", "주택가격기준년도", "대지지분(㎡)"];
    const optionalFields = ["전용면적(㎡)", "공급면적(㎡)"];
    let missingFields = [];

    // 필수 필드는 무조건 0으로 설정
    requiredFields.forEach(field => {
      if (updateData[field] === undefined) {
        updateData[field] = 0; // 없는 필드는 0으로 설정
        missingFields.push(field);
      }
    });

    // 선택적 필드는 데이터가 있을 때만 설정 (없으면 건너뜀)
    optionalFields.forEach(field => {
      // 이미 updateData에 있으면 아무것도 하지 않음
      // 없으면 그냥 건너뜀 (0으로 설정하지 않음)
    });

    if (missingFields.length > 0) {
      logger.info(`누락된 필수 필드를 0으로 설정: ${missingFields.join(', ')}`);
    }

    if (Object.keys(updateData).length === 0) {
      logger.warn(`업데이트할 유효한 데이터가 없음: ${record.id}`);
      return false;
    }

    logger.info(`📝 업데이트 예정 필드: ${Object.keys(updateData).join(', ')}`);
    await airtableBase(MULTI_UNIT_TABLE).update(record.id, updateData);
    
    const totalTime = Date.now() - startTime;
    logger.info(`✅ 에어테이블 업데이트 성공: ${record.id} (총 ${totalTime}ms)`);
    
    return true;
  } catch (error) {
    logger.error(`❌ 레코드 처리 실패 ${record.id}:`, error.message);
    if (error.stack) {
      logger.debug(`스택 트레이스:`, error.stack);
    }
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
    version: '3.8.0',
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
        <title>집합건물 서비스 관리 v3.8</title>
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
        <h1>🏗️ 집합건물 서비스 관리 v3.8</h1>
        
        <div class="info">
            <h3>📋 현재 설정</h3>
            <p><strong>뷰 ID:</strong> ${MULTI_UNIT_VIEW}</p>
            <p><strong>API 지연시간:</strong> ${API_DELAY/1000}초</p>
            <p><strong>스케줄:</strong> 1분마다 실행</p>
            <p><strong>날짜 정보:</strong> 사용승인일, 주택가격기준일 (ISO 형식으로 변환)</p>
            <p><strong>기타 정보:</strong> 용도지역, 주택가격, 대지지분</p>
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
            <p><strong>기타 정보:</strong> 용도지역, 주택가격, 대지지분</p>
            <p><strong>날짜 정보:</strong> 사용승인일, 주택가격기준일 (ISO 형식으로 변환)</p>
        </div>
        
        <h3>🆕 v3.8 업데이트</h3>
        <div class="fix">
            <p><strong>대지지분 API 변경:</strong> buldRlnmList API 사용</p>
            <p><strong>호수 매칭 개선:</strong> "1층201호" → "201" 추출 매칭</p>
            <p><strong>동이름 처리:</strong> 공란일 때 API 파라미터에서 제외</p>
            <p><strong>응답 구조 유연성:</strong> 다양한 VWorld API 응답 구조 지원</p>
        </div>
    </body>
    </html>
  `);
});

app.listen(PORT, () => {
  logger.info('🚀 집합건물 서비스 v3.8 시작됨');
  logger.info(`📡 포트: ${PORT}`);
  logger.info(`🌐 웹 인터페이스: http://localhost:${PORT}`);
  logger.info(`📋 사용 뷰: ${MULTI_UNIT_VIEW}`);
  logger.info(`⏱️ API 지연시간: ${API_DELAY/1000}초`);
  logger.info(`🔄 스케줄: 1분마다 실행`);
});

module.exports = app;