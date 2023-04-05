const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');

const { USADataRequest }= require('./services/api.js');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        const data = await USADataRequest();

        // 1
        await db[DATABASE_SCHEMA].api_data.insert({
            doc_record: data,
        });

        // 2 - a
        const result1 = data.data
            .filter(x => x.Year >= 2018 && x.Year <= 2020)
            .reduce((sum, item) => sum += item.Population, 0);
            
        console.log(`1) - Total USA population in 2018, 2019 and 2020 is ${result1}`);

        // 2 - b
        const result2 = await db[DATABASE_SCHEMA].api_data.findOne({
            is_active: true
        });

        const totalPopulation = result2.doc_record.data
            .filter(x => x.Year >= 2018 && x.Year <= 2020)
            .reduce((sum, item) => sum += item.Population, 0);

        console.log(`2) - Total USA population in 2018, 2019 and 2020 is ${totalPopulation}`);

        // 2 - c
        const result3 = await db.query(`
            SELECT SUM(cast(obj->>'Population' as integer)) total
            FROM   flavio_fgjj.api_data r, jsonb_array_elements(r.doc_record#>'{data}') obj
            WHERE  obj->>'Year' IN ('2018', '2019', '2020')
        `);

        console.log(`3) - Total USA population in 2018, 2019 and 2020 is ${result3[0].total}`);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();