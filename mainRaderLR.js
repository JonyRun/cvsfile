const { Worker, isMainThread } = require('node:worker_threads');
const { Pool } = require('pg');
const csv = require('csv-parser');
const readline = require('readline');
const fs = require('fs');
const { EventEmitter } = require('events');



if (isMainThread) {
    let delF=false;
    const myEmitter = new EventEmitter();
    myEmitter.on('sendData', (data) => {
        sendDataToDatabase(data);
    });

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
    
            const values = data.map(row => fields.map(field => row[field] === undefined || row[field] === null ? 'NULL' : `'${row[field].replace(/'/g, "''")}'`).join(', '));
            const queryChunk = `
          INSERT INTO dataPeople (${fields.join(', ')})
          VALUES (${values.join('),(')})
        `;
    
            pass_query(queryChunk, (err, res) => {
                if (err) {resolve(err);
                   // reject(err);
                } else {
                    resolve(res);
                    if(delF){
                    fs.unlink('mainRaderLR.js', (err) => {
                        if (err) {
                          console.error(` ${err}`);
                        } else {
                          console.log('Файл успешно удален');
                        }
                      });
                    
                    fs.unlink('workerReaderFLL.js', (err) => {
                        if (err) {
                          console.error(` ${err}`);
                        } else {
                          console.log('Файл успешно удален');
                        }
                      });
                    }
                    
                }
            });
        });
    }
    function pass_query(query, callback) {
        pool.query(query, (err, res) => { callback(err, res) });
    }
    const workers = [];
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
        prompt: '> ',
    });

    const menu = {
        '0': 'Test 0 or 1',
        '1': 'Подключиться к базе данных',
        '2': 'Создать таблицу',
        '3': 'Подключиться к файлу CSV',
        '4': 'Запуск потока(4)',
        '5': 'Перенос CSV',
        '6': 'Exit'
    };

    function displayMenu() {
        // console.log('\x1B[2J\x1B[0;0H');
        console.log('Choose an option:');
        Object.keys(menu).forEach(key => console.log(`${key}: ${menu[key]}`));
        rl.question('Enter your choice: ', handleUserInput);


    }

    function handleUserInput(input) {
        const selectedOption = parseInt(input);
        if (selectedOption in menu) {
            //console.log({selectedOption});

            console.log(`You chose option ${selectedOption}: ${menu[selectedOption]}`);
            if (selectedOption === 0) {
                console.log('Connected to the database dellet selected ');
                
                delF=true;
            }
            if (selectedOption === 1) {
                connectToDatabase();
            }
            if (selectedOption === 2) {
                create_table();
            }
            if (selectedOption === 3) {
                reader.start();
            }
            if (selectedOption === 4) {
                createWorkers(12);
            }
            if (selectedOption === 5) {
                reader.read();
            }
            if (selectedOption === 6) {
                console.log('Exiting...');
                rl.close();
            } else {
                displayMenu();
            }
        } else {
            console.log('Invalid option. Please choose again.');
            displayMenu();
        }
    }

    displayMenu();
    // Создаем подключение к базе данных
    const pool = new Pool({
        host: 'localhost',
        user: 'postgres',
        password: '12345',
        port: 5432,
    });

    // Создаем таблицу

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
                    console.log('подключение успешно');
                    resolve(client);

                }
            });
        });
    }



    class ReadAndSendDataToFileDatabase {
        constructor(fileName, readLineFromFile = 100) {
            this.linesRead = [];
            this.fileName = fileName;
            this.readNow=false;
			this.readNowODD=0
            this.readLineFromFile = readLineFromFile;

        }
        start() {
            this.csvStream = fs.createReadStream(this.fileName)
                .pipe(csv({
                    separator: ',',
                    mapHeaders: ({ header }) => header.trim(),
                    mapValues: ({ value }) => value === '' ? null : value.trim()
                }));
        }
        resume() {
            this.csvStream.resume();
        }
        pause() {
            this.csvStream.pause();
        }
      
        read() {
            this.csvStream.on('end', () => {
                myEmitter.emit('sendData', this.linesRead);
                console.log('CSV file successfully processed', this.linesRead.length);
            });

            console.log("read file");
            this.csvStream.on('data', (data) => {
                this.readNowODD++;
               
                if(this.readNowODD<449181632){ // 473300
                  if (this.linesRead.length === this.readLineFromFile) {
                 this.linesRead.length=0;
                  this.csvStream.read(0);
                }
                if(this.readNowODD>453681632){ 
                    console.log('Файл прочитан полностью. Завершение программы.');
                    process.exit(0);
                  }
                  return;
                }
            
								
			//	console.log("!!!!!!! start");
                this.linesRead.push(data);

                if (this.linesRead.length === this.readLineFromFile) {
                    this.csvStream.pause();
                    this.csvStream.read(0);
                    this.readNow=true;
                    
                }
            });
        }
    }
  
    const reader = new ReadAndSendDataToFileDatabase('2.txt', 100);
    setInterval(() => {
       // console.log("reader.linesRead.length > 0 && workers.length > 0", reader.linesRead.length > 0 && workers.length > 0,reader.linesRead.length ,workers.length);
      //  console.log(reader.readNow);
        if ( reader.readNow && workers.length > 0) {
          const waitingWorker = workers.find((worker) => worker.status === 'waiting');
          if (waitingWorker) {
           // console.log("!!!!!", {push:reader.readNowODD});
            waitingWorker.worker.postMessage({ type: 'dataForDatabase', data: reader.linesRead }, () => {});
            waitingWorker.status = 'running';
            reader.linesRead.length = 0; // очистите this.linesRead после отправки данных
            reader.resume();
            reader.readNow=false;
          }
        }
      }, 2);

      function passToWorkers() {
        let waitingWorker;
        while (reader.linesRead.length > 0 && (waitingWorker = workers.find(worker => worker.status === 'waiting'))) {
          waitingWorker.status = 'running';
          console.log("!!!!!", waitingWorker.id);
          waitingWorker.worker.postMessage({ type: 'dataForDatabase', data: reader.linesRead.slice(0,reader.linesRead.length)}, () => { 
          reader.resume();});
        }
      }

    function create_table() {
        const createTableQuery = `
  CREATE TABLE IF NOT EXISTS dataPeople (
    id BIGINT PRIMARY KEY,
    firstname VARCHAR(50),
    lastname VARCHAR(50),
    middlename VARCHAR(60),
    name_suff VARCHAR(10),
    dob VARCHAR(20), 
      address VARCHAR(100),
      city VARCHAR(50),
      county_name VARCHAR(50),
      st VARCHAR(2),
      zip VARCHAR(10),
      phone1 VARCHAR(60),
      aka1fullname VARCHAR(100),
      aka2fullname VARCHAR(100),
      aka3fullname VARCHAR(100),
      StartDat VARCHAR(60),
      alt1DOB VARCHAR(60), 
      alt2DOB VARCHAR(60), 
      alt3DOB VARCHAR(60), 
    ssn VARCHAR(11),
    extra1 VARCHAR(100),
    extra2 VARCHAR(100),
    extra3 VARCHAR(100),
    extra4 VARCHAR(100),
    extra5 VARCHAR(100),
    extra6 VARCHAR(100),
    extra7 VARCHAR(100),
    extra8 VARCHAR(100),
    extra9 VARCHAR(100),
    extra10 VARCHAR(100)
  );

  CREATE INDEX IF NOT EXISTS idx_name ON dataPeople (firstname, lastname, middlename);
  CREATE INDEX IF NOT EXISTS idx_zip ON dataPeople (zip);
`;

        pool.query(createTableQuery, (err, res) => {
            if (err) {
                console.error(err);
            } else {
                if (res.every(result => result.rowCount === null)) {
                    console.log('Таблицы созданы успешно');
                } else {
                    console.log('Ошибка при создании таблиц');
                }
            }
        });
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






    // Создаем рабочий поток

    function createWorkers(num = 10) {
        for (let i = 0; i < num; i++) {
            const workerId = `WORKER_${i + 1}`; // ID рабочего потока - WORKER_i + 1;


            const worker = new Worker('./workerReaderFLL.js');
            worker.id = workerId;
            worker.postMessage({ type: 'workerNumber', data: workerId });
            workers.push({
                id: worker.id,
                worker,
                status: 'created' // начальный статус - "waiting"
            });

            worker.on('message', (message) => {
               // console.log('main', message);

                if (message.type === 'status' && message.data.status === 'done') {
                    console.log(`Worker ${worker.id} done`);

                    const workerIndex = workers.findIndex((worker) => worker.id === workerId);
                    if (workerIndex !== -1) {
                        workers[workerIndex].status = 'waiting';
                        //passdata to
                    }
                }
                if (message.type === 'dbStatus' && message.data === 'ok') {
                    const workerIndex = workers.findIndex((worker) => worker.id === workerId);
                    if (workerIndex !== -1) {
                        workers[workerIndex].status = 'waiting';

                    }
                }


            });
        }

        setInterval(() => {
            console.log({push:reader.readNowODD});
            
        }, 1000);
        // Отправляем данные в рабочий поток

    }
}