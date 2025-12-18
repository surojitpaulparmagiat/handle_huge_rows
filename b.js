const mysql = require('mysql2/promise');
const {createReadStream} = require("node:fs");

require('dotenv').config();

const db_user = process.env.MYSQL_USER;
const db_password = process.env.MYSQL_PASSWORD;
const db_host = process.env.MYSQL_WRITE_HOST;
const db_name = process.env.MYSQL_DATABASE;

// MySQL Database Configuration
const DB_CONFIG = {
    host: db_host,
    port: 3306,
    user: db_user,
    password: db_password,
    database: db_name,
};
console.log("DB_CONFIG", DB_CONFIG);

(async () => {
    try {
        const connection = await mysql.createConnection({
            ...DB_CONFIG,
            infileStreamFactory: (p) => createReadStream(p),
        });
        await connection.query('select 1+1 as result');
        const d = await connection.query('SELECT count(*) from temp_aud_source_data');
        console.log("d", d)
        console.log("DB CONNECTED SUCCESSFULLY");
    }
    catch (error) {
        console.log("error", error)
        console.log("DB CONNECTION ERROR: ", error.message);
    }

})();


