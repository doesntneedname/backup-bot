import { google } from 'googleapis';
import { fetchMetabaseData } from './metabase.js';

const SHEET_BACKUP = 'Бекап';
const Q_PAYING = 640, Q_OPEN = 642, Q_ACT = 643;
const GREEN = { red: 0.85, green: 0.918, blue: 0.827 };
const RED = { red: 0.956, green: 0.80, blue: 0.80 };
const MS_GONE = 22 * 864e5;

export async function updateCompaniesInfo(auth, spreadsheetId) {
  const gs = google.sheets({ version: 'v4', auth });
  const today = stripTime(new Date());
  const todayTS = +today;

  const [payRaw, openRaw, actRaw] = await Promise.all([
    fetchMetabaseData(Q_PAYING),
    fetchMetabaseData(Q_OPEN),
    fetchMetabaseData(Q_ACT),
  ]);

  console.log(`📥 PAYING: ${payRaw.length}`);
  console.log(payRaw.slice(0, 10));
  console.log(`📥 OPEN: ${openRaw.length}`);
  console.log(openRaw.slice(0, 10));
  console.log(`📥 ACTIVE: ${actRaw.length}`);
  console.log(actRaw.slice(0, 10));

  const payMap = new Map(payRaw.map(r => [String(r.id), r]));
  const openMap = new Map(openRaw.map(r => [String(r['Company ID']), +r.Count]));
  const actMap = new Map(actRaw.map(r => [String(r['Company ID']), +r.Count]));

  const curIdsRes = await gs.spreadsheets.values.get({
    spreadsheetId, range: `${SHEET_BACKUP}!A2:A`
  });
  const curIds = (curIdsRes.data.values || []).map(r => String(r[0]));
  const curSet = new Set(curIds);

  const newcomers = payRaw.filter(r => {
    if (curSet.has(String(r.id))) return false;
    if (r.next_payment_at) {
      const payDate = parseDate(r.next_payment_at);
      if (todayTS - +payDate > MS_GONE) return false;
    }
    return true;
  });

  if (newcomers.length) {
    await gs.spreadsheets.values.append({
      spreadsheetId,
      range: `${SHEET_BACKUP}!A:A`,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: newcomers.map(c => [String(c.id)]) }
    });

    const sheetId = await getSheetId(gs, spreadsheetId);
    await colorRows(gs, spreadsheetId, curIds.length + 1, curIds.length + 1 + newcomers.length, GREEN);

    // 🧹 удаление дубликатов по колонке A
    const allRowsRes = await gs.spreadsheets.values.get({
      spreadsheetId,
      range: `${SHEET_BACKUP}!A2:A10000`
    });
    const allIds = (allRowsRes.data.values || []).map(r => r[0]);
    const seen = new Set();
    const duplicateIndexes = [];

    allIds.forEach((val, i) => {
      if (seen.has(val)) duplicateIndexes.push(i + 1);
      else seen.add(val);
    });

    if (duplicateIndexes.length) {
      await gs.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: {
          requests: duplicateIndexes.reverse().map(i => ({
            deleteDimension: {
              range: {
                sheetId,
                dimension: 'ROWS',
                startIndex: i + 1,
                endIndex: i + 2,
              }
            }
          }))
        }
      });
      console.log(`🧹 Удалено дубликатов: ${duplicateIndexes.length}`);
    }
  }

  const idsRes = await gs.spreadsheets.values.get({ spreadsheetId, range: `${SHEET_BACKUP}!A2:A` });
  const ids = idsRes.data.values.map(r => String(r[0]));
  const sheetId = await getSheetId(gs, spreadsheetId);
  const reqs = [];

  ids.forEach((id, idx) => {
    const meta = payMap.get(id);
    if (!meta) return;
    const row = idx + 1;
    const c = [];

    const added = openMap.get(id);
    if (!isNaN(added)) c.push({ c: 6, v: added, n: true }); // G

    c.push({ c: 1, v: meta.name }); // B

    const paid = +meta.paid_licenses_count;
    if (!isNaN(paid)) c.push({ c: 3, v: paid, n: true }); // D

    const active = actMap.get(id);
    if (!isNaN(active)) c.push({ c: 9, v: active, n: true }); // J

    c.push({ c: 12, v: meta['Plan__name'] || '' }); // M

    const price = String(meta['Combined Price, Discount'] ?? '').trim();
    if (price) c.push({ c: 13, v: price }); // N

    const mrr = +meta.mrr;
    if (!isNaN(mrr)) c.push({ c: 15, v: mrr, n: true }); // P

    if (meta.next_payment_at) {
      const date = parseDate(meta.next_payment_at);
      if (!isNaN(+date)) {
        const next = new Date(+date + 86400000);
        c.push({ c: 19, v: dateSerial(next), d: true }); // T
      }
    }

    c.forEach(o => reqs.push(cellReq(sheetId, row, o)));
  });

  if (reqs.length) {
    await gs.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: reqs } });
  }

  console.log(`✅ Обработка завершена: компаний: ${ids.length}`);
}

// ────────── helpers ────────────────────────

function dateSerial(d) {
  return Math.floor((d - Date.UTC(1899, 11, 30)) / 86400000);
}

function stripTime(d) {
  return new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
}

function parseDate(val) {
  if (val == null || val === '') return new Date(0);
  if (typeof val === 'number') {
    return stripTime(new Date(Date.UTC(1899, 11, 30) + val * 864e5));
  }
  if (/^\d{2}\.\d{2}\.\d{2,4}$/.test(val)) {
    const [d, m, y] = val.split('.');
    const yyyy = y.length === 2 ? `20${y}` : y;
    return stripTime(new Date(`${yyyy}-${m}-${d}`));
  }
  return stripTime(new Date(val));
}

async function getSheetId(gs, id) {
  return gs.spreadsheets.get({ spreadsheetId: id })
    .then(r => r.data.sheets.find(s => s.properties.title === SHEET_BACKUP).properties.sheetId);
}

function cellReq(sheetId, row, { c, v, n, d }) {
  const val = d || n ? { numberValue: v } : { stringValue: String(v) };
  const fmt = d ? { numberFormat: { type: 'DATE', pattern: 'dd.mm.yy' } } : undefined;
  return {
    updateCells: {
      rows: [{ values: [{ userEnteredValue: val, userEnteredFormat: fmt }] }],
      range: {
        sheetId,
        startRowIndex: row,
        endRowIndex: row + 1,
        startColumnIndex: c,
        endColumnIndex: c + 1
      },
      fields: 'userEnteredValue' + (d ? ',userEnteredFormat.numberFormat' : '')
    }
  };
}

function colorReq(sheetId, row, color) {
  return {
    repeatCell: {
      range: {
        sheetId,
        startRowIndex: row,
        endRowIndex: row + 1,
        startColumnIndex: 0,
        endColumnIndex: 2
      },
      cell: { userEnteredFormat: { backgroundColor: color } },
      fields: 'userEnteredFormat.backgroundColor'
    }
  };
}

async function colorRows(gs, sid, rs, re, color) {
  const sheetId = await getSheetId(gs, sid);
  await gs.spreadsheets.batchUpdate({
    spreadsheetId: sid,
    requestBody: {
      requests: [colorReq(sheetId, rs, color)]
    }
  });
}
