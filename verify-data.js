const { Client } = require('pg');
require('dotenv').config();

async function verify() {
    const client = new Client({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_NAME,
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT,
    });

    try {
        await client.connect();

        const countRes = await client.query('SELECT COUNT(*) FROM public.users');
        console.log(`Total Users: ${countRes.rows[0].count}`);

        const distRes = await client.query(`
      SELECT
        CASE
          WHEN age < 20 THEN '< 20'
          WHEN age >= 20 AND age < 40 THEN '20 to 40'
          WHEN age >= 40 AND age <= 60 THEN '40 to 60'
          WHEN age > 60 THEN '> 60'
        END as age_group,
        COUNT(*) as count
      FROM public.users
      GROUP BY age_group
    `);

        console.log('Age Distribution:');
        distRes.rows.forEach(row => {
            console.log(`${row.age_group}: ${row.count}`);
        });

    } catch (err) {
        console.error(err);
    } finally {
        await client.end();
    }
}

verify();
