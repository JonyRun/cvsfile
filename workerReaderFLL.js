const { parentPort } = require('node:worker_threads');
const { Pool } = require('pg');
let workerid = null;
let chunkSize = 4;

parentPort.on('message', async (message) => {
    //console.log(message);

    if (message.type === 'workerNumber') {
        workerid = message.data;
        await connectToDatabase()
            .then(() => {
                parentPort.postMessage({ type: 'dbStatus', data: 'ok' });
            })
            .catch((err) => {
                parentPort.postMessage({ type: 'dbStatus', data: 'error' });
            });
        return;
    }
    if (message.type === 'dataForDatabase') {
        console.log('Получены данные для отправки в базу данных');
        p_message(message.data, workerid);
        return;
    }
    return console.log('Неизвестный тип сообщения');
});





function p_message(data, workerId) {
  //  console.log(workerid, "get data", data.length);
    let line = data.length / chunkSize;
    const promises = []; // очистить массив promise
    for (let i = 0; i < chunkSize; i++) {
        const chunk = data.splice(0, line); // вырезать чанк из массива
        promises.push(sendDataToDatabase(chunk, workerId));
        chunk.length = 0;
    }
    Promise.all(promises).then((results) => {
      //  console.log(`${workerId} Загрузка в базу данных завершена. Загружено: ${results.length}`);
        parentPort.postMessage({
            type: 'status',
            data: {
                status: 'done',
                loadedCount: results.length,
                workerId: workerId
            }
        });
    });
}






const pool = new Pool({
    host: 'localhost',
    user: 'postgres',
    password: '12345',
    port: 5432,
    max:20
});
// Подключаемся к базе данных


// Проверяем подключение
function connectToDatabase() {
    return new Promise((resolve, reject) => {
        pool.connect((err, client, done) => {
            if (err) {
                console.error('Ошибка повторного подключения:', err);
                if (err.code === 'ECONNREFUSED') {
                    console.log('Повторное подключение через 1 секунду...');
                    setTimeout(() => connectToDatabase().then(resolve).catch(reject), 1000);
                } else {
                    reject(err);
                }
            } else {
                console.log('подключение успешно', workerid);

                resolve(client);
            }
        });
    });
}



let valuePass = 0;
const fieldTypes = {
    'ID': { type: 'INT', length: 50 },
    'firstname': { type: 'VARCHAR', length: 50 },
    'lastname': { type: 'VARCHAR', length: 50 },
    'middlename': { type: 'VARCHAR', length: 60 },
    'name_suff': { type: 'VARCHAR', length: 10 },
    'dob': { type: 'VARCHAR', length: 20 },
    'address': { type: 'VARCHAR', length: 100 },
    'city': { type: 'VARCHAR', length: 50 },
    'county_name': { type: 'VARCHAR', length: 50 },
    'st': { type: 'VARCHAR', length: 2 },
    'zip': { type: 'VARCHAR', length: 10 },
    'phone1': { type: 'VARCHAR', length: 60 },
    'aka1fullname': { type: 'VARCHAR', length: 100 },
    'aka2fullname': { type: 'VARCHAR', length: 100 },
    'aka3fullname': { type: 'VARCHAR', length: 100 },
    'StartDat': { type: 'VARCHAR', length: 60 },
    'alt1DOB': { type: 'VARCHAR', length: 60 },
    'alt2DOB': { type: 'VARCHAR', length: 60 },
    'alt3DOB': { type: 'VARCHAR', length: 60 },
    'ssn': { type: 'VARCHAR', length: 11 },
    'extra1': { type: 'VARCHAR', length: 100 },
    'extra2': { type: 'VARCHAR', length: 100 },
    'extra3': { type: 'VARCHAR', length: 100 },
    'extra4': { type: 'VARCHAR', length: 100 },
    'extra5': { type: 'VARCHAR', length: 100 },
    'extra6': { type: 'VARCHAR', length: 100 },
    'extra7': { type: 'VARCHAR', length: 100 },
    'extra8': { type: 'VARCHAR', length: 100 },
    'extra9': { type: 'VARCHAR', length: 100 },
    'extra10': { type: 'VARCHAR', length: 100 }
};




function sendDataToDatabase(data) {
  
    
    return new Promise((resolve, reject) => {
        const fields = [
            'ID',
            'firstname',
            'lastname',
            'middlename',
            'name_suff',
            'dob',
            'address',
            'city',
            'county_name',
            'st',
            'zip',
            'phone1',
            'aka1fullname',
            'aka2fullname',
            'aka3fullname',
            'StartDat',
            'alt1DOB',
            'alt2DOB',
            'alt3DOB',
            'ssn',
            'extra1',
            'extra2',
            'extra3',
            'extra4',
            'extra5',
            'extra6',
            'extra7',
            'extra8',
            'extra9',
            'extra10'
        ];
      
            const values = data.map(row => fields.map(field => row[field] === undefined || row[field] === null ? 'NULL' : `'${row[field].slice(0, fieldTypes[field].length).replace(/'/g, "''")}'`).join(', '));
        const queryChunk = `
       INSERT INTO dataPeople(${fields.join(', ')})
      VALUES (${values.join('),(')})
    `;
//console.log({data:data.length});

pass_query(queryChunk, (err, res) => {
    if (err) {
        resolve(err);
    } else {
        resolve(res);
       // console.log(`${workerid}Данные успешно записаны в базу данных ${valuePass += values.length}`);

    }
});
    });
}


function pass_query(query, callback) {
    pool.query(query, (err, res) => { callback(err, res) });
}

pool.on('error', (err) => {
    console.error(`Ошибка подключения к базе данных: ${err.message}`);
    try {
        if (err.code === '57P01') {
            rl.question('Соединение с базой данных потеряно. Хотите повторно подключиться или закрыть программу? (reconnect/exit): ', (answer) => {
                if (answer.trim().toLowerCase() === 'reconnect') {
                    connectToDatabase();
                } else if (answer.trim().toLowerCase() === 'exit') {
                    console.log('Закрытие программы.');
                    process.exit();
                } else {
                    console.log('Неправильный ответ. Пожалуйста, введите "reconnect" или "exit".');
                }
            });
        } else {
            console.error(`Неизвестная ошибка: ${err.message}`);
            process.exit();
        }
    } catch (error) {
        console.error(`Неизвестная ошибка: ${error.message}`);
        // дополнительная обработка ошибки
    }
});
process.on('uncaughtException', (err) => {
    console.error('Необработанная ошибка:', err.message);
    // дополнительная обработка ошибки
    setTimeout(connectToDatabase, 1000);
});



// parentPort.postMessage({ type: 'request', data: 'Hello from worker!' });
