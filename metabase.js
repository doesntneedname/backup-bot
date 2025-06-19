import axios from 'axios';
import dotenv from 'dotenv';
dotenv.config();

// Настройки для подключения к Metabase
const METABASE_URL = process.env.METABASE_URL;
const USERNAME = process.env.USERNAME;
const PASSWORD = process.env.PASSWORD;
const BASIC_AUTH_USER = process.env.BASIC_AUTH_USER;
const BASIC_AUTH_PASS = process.env.BASIC_AUTH_PASS;

// Функция для получения session token и данных
export async function fetchMetabaseData(questionId) {
  try {
    console.log('Запрашиваем session token...');
    // Запрашиваем session token
    const sessionResponse = await axios.post(
      `${METABASE_URL}/api/session`,
      { username: USERNAME, password: PASSWORD },
      {
        headers: { 'Content-Type': 'application/json' },
        auth: { username: BASIC_AUTH_USER, password: BASIC_AUTH_PASS },
      }
    );

    const sessionToken = sessionResponse.data.id;
    console.log('Session Token получен:', sessionToken);

    console.log('Запрашиваем все данные из Metabase одним запросом...');

    // Запрашиваем данные для вопроса с ID = questionId
    const dataResponse = await axios.post(
      `${METABASE_URL}/api/card/${questionId}/query`,
      {},
      {
        headers: {
          'Content-Type': 'application/json',
          'X-Metabase-Session': sessionToken,
        },
        params: {
          limit: 1000,
          offset: 0,
        },
        auth: {
          username: BASIC_AUTH_USER,
          password: BASIC_AUTH_PASS,
        },
      }
    );

    const data = dataResponse.data.data;
    const rows = data?.rows || [];
    const columns = data?.results_metadata?.columns || [];

    const mappedData = rows.map(row =>
      row.reduce((acc, value, index) => {
        acc[columns[index].display_name || columns[index].name] = value;
        return acc;
      }, {})
    );

    return mappedData;
  } catch (error) {
    console.error('Ошибка при запросе данных:', error.message);
    return [];
  }
}
