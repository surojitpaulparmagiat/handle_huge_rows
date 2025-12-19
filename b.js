const { getDbConnection } = require('./db_connection');

(async () => {
    try {
        const connection = await getDbConnection();
        const result = await connection.execute('SELECT COUNT(*) FROM temp_aud_source_data');
        const result2 = await connection.execute('SELECT COUNT(*) FROM temp_aud_source_data_import_sessions');
        await mig(connection);
        console.log('result is: ', result);
        console.log('result2 is: ', result2);
        await connection.end();
    } catch (error) {
        console.log('DB CONNECTION ERROR: ', error.message);
    }
})();



async function mig(connection) {
    const al = connection.execute(`alter table temp_aud_source_data_import_sessions add column progress_status json`);
    // await connection.execute(`truncate table temp_aud_source_data`);
}