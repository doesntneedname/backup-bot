import dotenv from 'dotenv';
import { google } from 'googleapis';
import cron from 'node-cron';
import { updateCompaniesInfo } from './updateinfo.js';

dotenv.config();

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

async function authorize() {
  const auth = new google.auth.GoogleAuth({
    keyFile: './service-account.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return await auth.getClient();
}

async function main() {
  console.log(`⏰ Запуск задачи: ${new Date().toISOString()}`);
  const auth = await authorize();
  await updateCompaniesInfo(auth, SPREADSHEET_ID);
}

// запуск ежедневно в 00:00
cron.schedule('0 0 * * *', () => {
  main().catch(err => {
    console.error('❌ Ошибка в расписании:', err);
  });
});

if (process.env.NODE_ENV !== 'production') {
  main().catch(err => {
    console.error('❌ Ошибка при запуске:', err);
  });
}
main()