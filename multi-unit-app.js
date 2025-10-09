// multi-unit-app.js - Main application file (개선판)
require('dotenv').config({ path: '/root/goldenrabbit/.env' });
const express = require('express');
const cron = require('node-cron');
const axios = require('axios');
const Airtable = require('airtable');
const convert = require('xml-js');
const path = require('path');
const fs = require('fs');
const nodemailer = require('nodemailer');

const app = express();
const PORT = process.env.MULTI_UNIT_BUILDING_SERVICE_PORT || 3003;

// ============================================
// 재시도 이력 저장 (메모리 기반)
// ============================================
const retryHistory = new Map();
const MAX_RETRY_ATTEMPTS = 5;
const RETRY_RESET_DAYS = 7;

// 영구 에러 패턴
const PERMANENT_ERROR_PATTERNS = [
  'Hostname/IP does not match',
  'certificate',
  'SSL',
  'CERT',
  '잘못된 주소 형식',
  '주소 없음',
  'Unknown field name',
  'Insufficient permissions',
  'Maximum execution time',
  'does not have a field',
  'Invalid permissions',
  '해당동 총층수', // Multi-Unit 특정 에러
];

// ============================================
// 이메일 설정
// ============================================
const emailTransporter = nodemailer.createTransport({
  host: process.env.SMTP_SERVER,
  port: parseInt(process.env.SMTP_PORT),
  secure: false,
  auth: {
    user: process.env.EMAIL_ADDRESS,
    pass: process.env.EMAIL_PASSWORD
  }
});

// ============================================
// 로그 설정
// ============================================
const logDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const LOG_LEVELS = {
  'debug': 0,
  'info': 1,
  'warn': 2,
  'error': 3
};

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

// ============================================
// 재시도 관리 함수
// ============================================

function isPermanentError(error) {
  const errorMsg = error.message || String(error);
  return PERMANENT_ERROR_PATTERNS.some(pattern => 
    errorMsg.includes(pattern)
  );
}

function canRetry(recordId) {
  const history = retryHistory.get(recordId);
  
  if (!history) return true;
  
  if (history.failed) {
    const daysSinceLastAttempt = (Date.now() - history.lastAttempt.getTime()) / (1000 * 60 * 60 * 24);
    if (daysSinceLastAttempt >= RETRY_RESET_DAYS) {
      retryHistory.delete(recordId);
      logger.info(`재시도 카운터 리셋: ${recordId} (${RETRY_RESET_DAYS}일 경과)`);
      return true;
    }
    return false;
  }
  
  return history.attempts < MAX_RETRY_ATTEMPTS;
}

function recordRetryAttempt(recordId, success, isPermanent = false) {
  const history = retryHistory.get(recordId) || { 
    attempts: 0, 
    lastAttempt: new Date(), 
    failed: false 
  };
  
  if (success) {
    retryHistory.delete(recordId);
    logger.info(`✅ 레코드 성공, 재시도 이력 삭제: ${recordId}`);
  } else {
    if (isPermanent) {
      history.attempts = MAX_RETRY_ATTEMPTS;
      history.failed = true;
      logger.warn(`⛔ 영구 에러 발생, 재시도 안함: ${recordId}`);
    } else {
      history.attempts += 1;
      history.lastAttempt = new Date();
      
      if (history.attempts >= MAX_RETRY_ATTEMPTS) {
        history.failed = true;
        logger.warn(`❌ 레코드 최대 재시도 횟수 도달: ${recordId} (${history.attempts}회)`);
      } else {
        logger.info(`재시도 기록: ${recordId} - 시도 ${history.attempts}/${MAX_RETRY_ATTEMPTS}`);
      }
    }
    
    retryHistory.set(recordId, history);
  }
}

async function sendFailureNotification(failedRecords) {
  if (failedRecords.length === 0) return;
  
  try {
    const recordsList = failedRecords.map(r => 
      `- ${r['지번 주소']} ${r['동']} ${r['호수']} (레코드 ID: ${r.id})`
    ).join('\n');
    
    const mailOptions = {
      from: process.env.EMAIL_ADDRESS,
      to: process.env.NOTIFICATION_EMAIL_TO || process.env.EMAIL_ADDRESS,
      subject: `[집합건물 서비스] ${failedRecords.length}개 레코드 처리 실패`,
      text: `
다음 집합건물 레코드들이 ${MAX_RETRY_ATTEMPTS}회 재시도 후에도 처리에 실패했습니다:

${recordsList}

총 실패 레코드: ${failedRecords.length}개
발생 시각: ${new Date().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' })}

조치 필요:
1. 에어테이블에서 해당 레코드의 주소/동/호수 정보 확인
2. 정보가 올바른지 확인
3. 필요시 수동으로 정보 입력
      `,
      html: `
<h2>집합건물 정보 수집 실패 알림</h2>
<p>다음 집합건물 레코드들이 <strong>${MAX_RETRY_ATTEMPTS}회 재시도</strong> 후에도 처리에 실패했습니다:</p>
<ul>
${failedRecords.map(r => `<li>${r['지번 주소']} ${r['동']} ${r['호수']} <small>(레코드 ID: ${r.id})</small></li>`).join('')}
</ul>
<p><strong>총 실패 레코드:</strong> ${failedRecords.length}개</p>
<p><strong>발생 시각:</strong> ${new Date().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' })}</p>
      `
    };
    
    await emailTransporter.sendMail(mailOptions);
    logger.info(`📧 실패 알림 이메일 발송 완료: ${failedRecords.length}개 레코드`);
  } catch (error) {
    logger.error('📧 이메일 발송 실패:', error.message);
  }
}

// ============================================
// 로그 정리
// ============================================
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

// ============================================
// 에어테이블 설정
// ============================================
const airtableBase = new Airtable({
  apiKey: process.env.AIRTABLE_ACCESS_TOKEN || process.env.AIRTABLE_API_KEY
}).base('appQkFdB8TdPVNWdz');

const MULTI_UNIT_TABLE = 'tblT28nHoneqlbgBh';
const MULTI_UNIT_VIEW = 'viwOs5jlYkIGPZ142';

// API 설정
const PUBLIC_API_KEY = process.env.PUBLIC_API_KEY;
const GOOGLE_SCRIPT_URL = process.env.GOOGLE_SCRIPT_URL;
const VWORLD_APIKEY = process.env.VWORLD_APIKEY;

const API_DELAY = 250; // 초당 4회 (안전마진)
const MAX_RETRIES = 2;
const RETRY_DELAY = 3000;

// ============================================
// 유틸리티 함수
// ============================================

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
  
  const year = dateStr.substring(0, 4);
  const month = dateStr.substring(4, 6);
  const day = dateStr.substring(6, 8);
  const formattedDate = `${year}-${month}-${day}`;
  
  const date = new Date(`${formattedDate}T00:00:00.000Z`);
  if (isNaN(date.getTime())) {
    logger.warn(`잘못된 날짜 형식: ${dateStr}`);
    return null;
  }
  
  return date.toISOString();
};

const extractHoNumber = (hoStr) => {
  if (!hoStr || typeof hoStr !== 'string') return '';
  
  const hoMatch = hoStr.match(/(\d+)호$/);
  if (hoMatch) {
    return hoMatch[1];
  }
  
  return extractNumbersOnly(hoStr);
};

const processDongHo = (dongNm, hoNm) => {
  let dongVariations = [];
  if (dongNm && dongNm.trim() !== '') {
    dongVariations.push(dongNm.trim());
    
    const dongNumbers = extractNumbersOnly(dongNm);
    if (dongNumbers !== dongNm.trim()) {
      dongVariations.push(dongNumbers);
    }
    
    const dongWithoutSuffix = dongNm.trim().replace(/동$/, '');
    if (dongWithoutSuffix !== dongNm.trim() && dongWithoutSuffix !== dongNumbers) {
      dongVariations.push(dongWithoutSuffix);
    }
  } else {
    dongVariations.push('');
  }
  
  let hoVariations = [];
  if (hoNm && hoNm.trim() !== '') {
    hoVariations.push(hoNm.trim());
    
    const hoNumber = extractHoNumber(hoNm);
    if (hoNumber !== hoNm.trim() && hoNumber !== '') {
      hoVariations.push(hoNumber);
    }
    
    const hoWithoutSuffix = hoNm.trim().replace(/호$/, '');
    if (hoWithoutSuffix !== hoNm.trim() && hoWithoutSuffix !== hoNumber) {
      hoVariations.push(hoWithoutSuffix);
    }
  } else {
    hoVariations.push('');
  }
  
  return { dongVariations, hoVariations };
};

// ============================================
// API 호출 함수들 (순차 처리로 변경)
// ============================================

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
    
    if (dongNm || hoNm) {
      const numericDong = extractNumbersOnly(dongNm || '');
      const numericHo = extractNumbersOnly(hoNm || '');
      
      logger.info(`🔄 2단계 면적 정보 조회 시도 - 숫자만 추출: 동='${numericDong}', 호='${numericHo}'`);
      
      if (numericDong || numericHo) {
        await delay(API_DELAY);
        
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
    
    logger.info(`🔄 3단계 면적 정보 조회 시도 - 동/호 파라미터 없이`);
    
    await delay(API_DELAY);
    
    const fallbackResponse = await axios.get('https://apis.data.go.kr/1613000/BldRgstHubService/getBrExposPubuseAreaInfo', {
      params: {
        serviceKey: PUBLIC_API_KEY,
        sigunguCd: codeData.시군구코드,
        bjdongCd: codeData.법정동코드,
        bun: codeData.번,
        ji: codeData.지,
        _type: 'json',
        numOfRows: 100,
        pageNo: 1
      },
      timeout: 30000
    });
    
    const fallbackTotalCount = fallbackResponse.data?.response?.body?.totalCount || 0;
    logger.info(`3단계 면적 정보 조회 결과: totalCount=${fallbackTotalCount}`);
    
    if (fallbackTotalCount > 0) {
      logger.info(`✅ 3단계 면적 정보 조회 성공`);
      return fallbackResponse.data;
    }
    
    logger.warn(`❌ 모든 단계에서 면적 정보 조회 실패`);
    return null;
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

    const jsonData = convert.xml2js(response.data, { compact: true, spaces: 2, textKey: '_text' });
    
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
      }
    }
    
    return { 용도지역: null, 토지면적: null };
  } catch (error) {
    logger.error(`❌ VWorld 토지특성 조회 실패 (PNU: ${pnu}):`, error.message);
    return { 용도지역: null, 토지면적: null };
  }
};

const getHousingPriceInfo = async (pnu, dongNm, hoNm) => {
  try {
    logger.info(`🏠 VWorld 주택가격 정보 조회 시작 - PNU: ${pnu}, 동: ${dongNm}, 호: ${hoNm}`);
    
    const { dongVariations, hoVariations } = processDongHo(dongNm, hoNm);
    
    for (const processDong of dongVariations) {
      for (const processHo of hoVariations) {
        logger.info(`주택가격 시도: 동='${processDong}', 호='${processHo}'`);
        
        const result = await tryGetHousingPrice(pnu, processDong, processHo);
        if (result.주택가격만원 > 0) {
          logger.info(`✅ 주택가격 성공: 동='${processDong}', 호='${processHo}', 가격=${result.주택가격만원}만원`);
          return result;
        }
      }
    }
    
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

const tryGetHousingPrice = async (pnu, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    const params = {
      key: VWORLD_APIKEY,
      pnu: pnu,
      format: 'json',
      numOfRows: 30,
      pageNo: 1
    };
    
    if (dongNm && dongNm.trim() !== '') {
      params.dongNm = dongNm.trim();
    }
    
    if (hoNm && hoNm.trim() !== '') {
      params.hoNm = hoNm.trim();
    }
    
    const response = await axios.get('https://api.vworld.kr/ned/data/getApartHousingPriceAttr', {
      params: params,
      timeout: 30000
    });
    
    const totalCount = response.data?.apartHousingPrices?.totalCount || 0;
    if (totalCount === 0 || totalCount === "0") {
      return {
        주택가격만원: 0,
        주택가격기준년도: 0
      };
    }
    
    let items = [];
    
    if (response.data?.apartHousingPrices?.field) {
      const rawItems = response.data.apartHousingPrices.field;
      items = Array.isArray(rawItems) ? rawItems : [rawItems];
    }
    
    if (items.length > 0) {
      items.sort((a, b) => {
        const yearA = parseInt(a.stdrYear || '0');
        const yearB = parseInt(b.stdrYear || '0');
        return yearB - yearA;
      });
      
      const latestItem = items[0];
      const pblntfPc = latestItem.pblntfPc || '';
      const stdrYear = latestItem.stdrYear || '';
      
      let priceValue = parseInt(pblntfPc) || 0;
      
      if (priceValue > 1000000) {
        priceValue = Math.round(priceValue / 10000);
      }
      
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

const getLandShareInfo = async (pnu, dongNm, hoNm) => {
  try {
    logger.info(`🌍 VWorld 대지지분 정보 조회 시작 - PNU: ${pnu}, 동: ${dongNm}, 호: ${hoNm}`);
    
    const { dongVariations, hoVariations } = processDongHo(dongNm, hoNm);
    
    if (!dongNm || dongNm.trim() === '') {
      dongVariations.push("0000");
    }
    
    for (const processDong of dongVariations) {
      for (const processHo of hoVariations) {
        logger.info(`대지지분 시도: 동='${processDong}', 호='${processHo}'`);
        
        const result = await tryGetLandShare(pnu, processDong, processHo);
        if (result !== null) {
          logger.info(`✅ 대지지분 성공: 동='${processDong}', 호='${processHo}', 지분=${result}`);
          return result;
        }
      }
    }
    
    logger.info(`🔄 동 파라미터 없이 대지지분 재시도...`);
    const resultWithoutDong = await tryGetLandShare(pnu, '', hoVariations[0]);
    if (resultWithoutDong !== null) {
      logger.info(`✅ 대지지분 성공 (동 파라미터 없이): 호='${hoVariations[0]}', 지분=${resultWithoutDong}`);
      return resultWithoutDong;
    }
    
    logger.warn(`❌ 모든 동/호 변형으로 대지지분 조회 실패`);
    return null;
  } catch (error) {
    logger.error(`❌ VWorld 대지지분 조회 실패 (PNU: ${pnu}):`, error.message);
    return null;
  }
};

const tryGetLandShare = async (pnu, dongNm, hoNm) => {
  try {
    await delay(API_DELAY);
    
    const params = {
      key: VWORLD_APIKEY,
      pnu: pnu,
      format: 'json',
      numOfRows: 10,
      pageNo: 1
    };
    
    if (dongNm && dongNm.trim() !== '') {
      params.buldDongNm = dongNm.trim();
    }
    
    if (hoNm && hoNm.trim() !== '') {
      params.buldHoNm = hoNm.trim();
    }
    
    const response = await axios.get('https://api.vworld.kr/ned/data/buldRlnmList', {
      params: params,
      timeout: 30000
    });
    
    const totalCount = response.data?.ldaregVOList?.totalCount || response.data?.buldRlnmVOList?.totalCount || 0;
    if (totalCount === 0 || totalCount === "0") {
      return null;
    }
    
    let items = [];
    
    if (response.data?.ldaregVOList?.ldaregVOList) {
      const rawItems = response.data.ldaregVOList.ldaregVOList;
      items = Array.isArray(rawItems) ? rawItems : [rawItems];
    } 
    else if (response.data?.buldRlnmVOList?.buldRlnmVOList) {
      const rawItems = response.data.buldRlnmVOList.buldRlnmVOList;
      items = Array.isArray(rawItems) ? rawItems : [rawItems];
    }
    
    if (items.length > 0) {
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

// ============================================
// 데이터 처리 함수들
// ============================================

const extractItems = (data) => {
  if (!data || !data.response || !data.response.body || !data.response.body.items || !data.response.body.items.item) {
    return [];
  }
  
  const items = data.response.body.items.item;
  return Array.isArray(items) ? items : [items];
};

const isDongMatch = (apiDong, inputDong) => {
  if (!inputDong || inputDong.trim() === '') return true;
  
  const normalizedInput = extractNumbersOnly(inputDong);
  const normalizedApi = extractNumbersOnly(apiDong || '');
  
  return normalizedInput === normalizedApi;
};

const processMultiUnitBuildingData = (recapData, titleData, areaData, landCharacteristics, housingPrice, landShare, dongNm, hoNm) => {
  const result = {};
  
  const hasRecapData = recapData && recapData.response && recapData.response.body && recapData.response.body.totalCount && parseInt(recapData.response.body.totalCount) > 0;
  
  if (hasRecapData) {
    logger.info('총괄표제부 데이터 처리 중 (아파트 등)');
    
    const recapItems = extractItems(recapData);
    if (recapItems.length > 0) {
      const recap = recapItems[0];
      
      if (recap.platArea) result["대지면적(㎡)"] = parseFloat(recap.platArea);
      if (recap.totArea) result["연면적(㎡)"] = parseFloat(recap.totArea);
      if (recap.vlRatEstmTotArea) result["용적률산정용연면적(㎡)"] = parseFloat(recap.vlRatEstmTotArea);
      if (recap.archArea) result["건축면적(㎡)"] = parseFloat(recap.archArea);
      if (recap.bcRat) result["건폐율(%)"] = parseFloat(recap.bcRat);
      if (recap.vlRat) result["용적률(%)"] = parseFloat(recap.vlRat);
      if (recap.bldNm) result["건물명"] = recap.bldNm;
      if (recap.totPkngCnt) result["총주차대수"] = parseInt(recap.totPkngCnt);
      
      const 사용승인일 = formatDateISO(recap.useAprDay);
      if (사용승인일) result["사용승인일"] = 사용승인일;
      
      const 총세대수 = recap.hhldCnt || '0';
      const 총가구수 = recap.fmlyCnt || '0';
      const 총호수 = recap.hoCnt || '0';
      result["총 세대/가구/호"] = `${총세대수}/${총가구수}/${총호수}`;
      
      if (recap.mainBldCnt) result["주건물수"] = parseInt(recap.mainBldCnt);
    }
    
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
        
        if (matchingDong.useAprDay) {
          const 동사용승인일 = formatDateISO(matchingDong.useAprDay);
          if (동사용승인일) result["사용승인일"] = 동사용승인일;
        }
      }
    }
    
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
    logger.info('총괄표제부 없음, 표제부 데이터 처리 중 (빌라, 다세대 등)');
    
    const titleItems = extractItems(titleData);
    if (titleItems.length > 0) {
      const mainInfo = titleItems[0];
      
      if (mainInfo.newPlatPlc) result["도로명주소"] = mainInfo.newPlatPlc;
      if (mainInfo.bldNm) result["건물명"] = mainInfo.bldNm;
      if (mainInfo.heit) result["높이(m)"] = parseFloat(mainInfo.heit);
      if (mainInfo.strctCdNm) result["주구조"] = mainInfo.strctCdNm;
      if (mainInfo.roofCdNm) result["지붕"] = mainInfo.roofCdNm;
      if (mainInfo.mainPurpsCdNm) result["주용도"] = mainInfo.mainPurpsCdNm;
      
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
      
      const 지상층수 = mainInfo.grndFlrCnt || '0';
      const 지하층수 = mainInfo.ugrndFlrCnt || '0';
      result["총층수"] = `-${지하층수}/${지상층수}`;
      
      const 세대수 = mainInfo.hhldCnt || '0';
      const 가구수 = mainInfo.fmlyCnt || '0';
      const 호수 = mainInfo.hoCnt || '0';
      result["해당동 세대/가구/호"] = `${세대수}/${가구수}/${호수}`;
      result["총 세대/가구/호"] = `${세대수}/${가구수}/${호수}`;
      
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
      
      result["주건물수"] = 1;
    }
  }
  
  let 전용면적 = null;
  let 공용면적 = 0;
  let 공급면적 = null;

  if (areaData) {
    const areaItems = extractItems(areaData);
    logger.info(`📏 면적 정보 항목 수: ${areaItems.length}`);
    
    if (areaItems.length > 0) {
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
      
      if (tempArea전용 > 0) {
        전용면적 = tempArea전용;
      }
      
      공용면적 = tempArea공용;
      
      if (전용면적 !== null) {
        공급면적 = 전용면적 + 공용면적;
      }
    }
  }

  if (전용면적 !== null) {
    result["전용면적(㎡)"] = 전용면적;
  }

  if (공급면적 !== null) {
    result["공급면적(㎡)"] = 공급면적;
  }

  logger.info(`📊 최종 면적 정보: 전용=${전용면적 !== null ? 전용면적 : '없음'}㎡, 공용=${공용면적}㎡, 공급=${공급면적 !== null ? 공급면적 : '없음'}㎡`);
  
  if (landCharacteristics) {
    if (landCharacteristics.용도지역) {
      result["용도지역"] = landCharacteristics.용도지역;
    }
    if (landCharacteristics.토지면적) {
      result["토지면적(㎡)"] = landCharacteristics.토지면적;
    }
  }
  
  if (housingPrice) {
    if (housingPrice.주택가격만원 !== undefined) {
      result["주택가격(만원)"] = housingPrice.주택가격만원;
    } else {
      result["주택가격(만원)"] = 0;
    }
    
    if (housingPrice.주택가격기준년도 !== undefined) {
      result["주택가격기준년도"] = housingPrice.주택가격기준년도;
    } else {
      result["주택가격기준년도"] = 0;
    }
  } else {
    result["주택가격(만원)"] = 0;
    result["주택가격기준년도"] = 0;
  }
  
  if (landShare !== null) {
    result["대지지분(㎡)"] = landShare;
  } else {
    result["대지지분(㎡)"] = 0;
  }
  
  return result;
};

// ============================================
// 레코드 처리 (순차 API 호출)
// ============================================

const processMultiUnitBuildingRecord = async (record) => {
  if (!canRetry(record.id)) {
    logger.info(`⏭️ 레코드 건너뜀 (최대 재시도 횟수 초과): ${record.id}`);
    return { success: false, skipped: true };
  }

  try {
    const 지번주소 = record['지번 주소'];
    const 동 = record['동'] || '';
    const 호수 = record['호수'];

    logger.info(`🏗️ 레코드 처리 시작 (시도 ${(retryHistory.get(record.id)?.attempts || 0) + 1}/${MAX_RETRY_ATTEMPTS}): ${record.id} - ${지번주소} ${동} ${호수}`);

    const parsedAddress = parseAddress(지번주소);
    if (parsedAddress.error) {
      logger.error(`주소 파싱 실패: ${parsedAddress.error}`);
      recordRetryAttempt(record.id, false, true);
      return { success: false, skipped: false };
    }

    const buildingCodes = await getBuildingCodes(parsedAddress);
    const pnu = generatePNU(buildingCodes);
    
    if (!pnu) {
      logger.error(`PNU 생성 실패: ${record.id}`);
      recordRetryAttempt(record.id, false, true);
      return { success: false, skipped: false };
    }
    
    logger.info(`📍 생성된 PNU: ${pnu}`);

    // 순차적 API 호출 (병렬 제거)
    logger.info(`📡 API 데이터 수집 시작 (순차 처리)...`);
    
    const startTime = Date.now();
    
    const recapData = await getBuildingRecapInfo(buildingCodes);
    const titleData = await getBuildingTitleInfo(buildingCodes);
    const areaData = await getBuildingAreaInfo(buildingCodes, 동, 호수);
    const exposData = await getBuildingExposInfo(buildingCodes, 동, 호수);
    const landCharacteristics = pnu ? await getLandCharacteristics(pnu) : { 용도지역: null, 토지면적: null };
    const landShare = pnu ? await getLandShareInfo(pnu, 동, 호수) : null;
    const housingPrice = pnu ? await getHousingPriceInfo(pnu, 동, 호수) : { 주택가격만원: 0, 주택가격기준년도: 0 };
    
    const apiTime = Date.now() - startTime;
    logger.info(`⚡ API 데이터 수집 완료 (${apiTime}ms)`);

    const processedData = processMultiUnitBuildingData(
      recapData, titleData, areaData, landCharacteristics, housingPrice, landShare, 동, 호수
    );

    if (Object.keys(processedData).length === 0) {
      logger.warn(`처리된 데이터가 없습니다: ${record.id}`);
      recordRetryAttempt(record.id, false, false);
      return { success: false, skipped: false };
    }

    const updateData = {};
    Object.keys(processedData).forEach(key => {
      const value = processedData[key];
      if (value !== null && value !== undefined) {
        updateData[key] = value;
      }
    });

    const requiredFields = ["주택가격(만원)", "주택가격기준년도", "대지지분(㎡)"];
    requiredFields.forEach(field => {
      if (updateData[field] === undefined) {
        updateData[field] = 0;
      }
    });

    if (Object.keys(updateData).length === 0) {
      logger.warn(`업데이트할 유효한 데이터가 없음: ${record.id}`);
      recordRetryAttempt(record.id, false, false);
      return { success: false, skipped: false };
    }

    logger.info(`📝 업데이트 예정 필드: ${Object.keys(updateData).join(', ')}`);
    await airtableBase(MULTI_UNIT_TABLE).update(record.id, updateData);
    
    const totalTime = Date.now() - startTime;
    logger.info(`✅ 에어테이블 업데이트 성공: ${record.id} (총 ${totalTime}ms)`);
    
    recordRetryAttempt(record.id, true);
    return { success: true, skipped: false };
  } catch (error) {
    logger.error(`❌ 레코드 처리 실패 ${record.id}:`, error.message);
    
    const isPermanent = isPermanentError(error);
    recordRetryAttempt(record.id, false, isPermanent);
    
    return { success: false, skipped: false };
  }
};

// ============================================
// 메인 작업 실행
// ============================================

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
      return { total: 0, success: 0, failed: 0, skipped: 0 };
    }

    const recordData = allRecords.map(record => ({
      id: record.id,
      '지번 주소': record.get('지번 주소') || '',
      '동': record.get('동') || '',
      '호수': record.get('호수') || ''
    }));

    const processableRecords = recordData.filter(record => canRetry(record.id));
    
    if (processableRecords.length === 0) {
      logger.info('✅ 모든 레코드가 재시도 제한 초과 상태입니다');
      return { total: recordData.length, success: 0, failed: 0, skipped: recordData.length };
    }
    
    logger.info(`📊 처리 가능한 레코드: ${processableRecords.length}/${recordData.length}개`);

    let successCount = 0;
    let failedCount = 0;
    let skippedCount = recordData.length - processableRecords.length;
    const newlyFailedRecords = [];

    for (let i = 0; i < processableRecords.length; i++) {
      const record = processableRecords[i];
      
      try {
        logger.info(`\n📍 [${i + 1}/${processableRecords.length}] 처리 중: ${record.id}`);
        const result = await processMultiUnitBuildingRecord(record);
        
        if (result.skipped) {
          skippedCount++;
        } else if (result.success) {
          successCount++;
        } else {
          failedCount++;
          const history = retryHistory.get(record.id);
          if (history && history.failed && history.attempts === MAX_RETRY_ATTEMPTS) {
            newlyFailedRecords.push(record);
          }
        }

        if (i < processableRecords.length - 1) {
          await delay(API_DELAY);
        }

      } catch (error) {
        logger.error(`❌ 레코드 처리 중 예외 발생 ${record.id}:`, error.message);
        failedCount++;
      }
    }

    if (newlyFailedRecords.length > 0) {
      await sendFailureNotification(newlyFailedRecords);
    }

    logger.info(`\n🎉 작업 완료!`);
    logger.info(`📊 처리 결과: ${recordData.length}개 중 ${successCount}개 성공, ${failedCount}개 실패, ${skippedCount}개 건너뜀`);
    if (processableRecords.length > 0) {
      logger.info(`📈 성공률: ${((successCount / processableRecords.length) * 100).toFixed(1)}%`);
    }

    return { total: recordData.length, success: successCount, failed: failedCount, skipped: skippedCount };
  } catch (error) {
    logger.error('❌ 작업 실행 중 오류:', error.message);
    return { total: 0, success: 0, failed: 0, skipped: 0, error: error.message };
  }
};

// ============================================
// 스케줄링 - 매시간 실행
// ============================================

cron.schedule('0 * * * *', async () => {
  logger.debug('⏰ 정기 작업 확인 중...');

  try {
    const sampleRecords = await airtableBase(MULTI_UNIT_TABLE)
      .select({
        view: MULTI_UNIT_VIEW,
        maxRecords: 10
      })
      .all();

    if (sampleRecords.length === 0) {
      logger.debug('✅ 처리할 레코드 없음, 작업 건너뜀');
      return;
    }
    
    const processableRecords = sampleRecords.filter(record => canRetry(record.id));
    
    if (processableRecords.length === 0) {
      logger.debug('✅ 모든 레코드가 최대 재시도 횟수 초과 상태, 작업 건너뜀');
      return;
    }

    logger.info('🎯 처리 가능한 집합건물 레코드 발견, 작업 실행 중...');
    logger.info(`   - 처리 가능: ${processableRecords.length}/${sampleRecords.length}개`);
    await runMultiUnitBuildingJob();
  } catch (error) {
    logger.error('❌ 정기 작업 확인 중 오류 발생:', error.message);
  }
});

// ============================================
// Express 설정
// ============================================

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    service: 'multi-unit-building-service',
    timestamp: new Date().toISOString(),
    version: '4.0.0-improved',
    features: {
      retry_limit: MAX_RETRY_ATTEMPTS,
      retry_reset_days: RETRY_RESET_DAYS,
      schedule: '매시간 (0 * * * *)',
      api_delay: `${API_DELAY}ms (초당 4회)`,
      email_notification: 'Enabled',
      sequential_api: 'Enabled (병렬 제거)'
    }
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