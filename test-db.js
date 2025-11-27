const { Client } = require('pg');
require('dotenv').config();

async function checkDatabase() {
    console.log('Testing connection to PostgreSQL...');

    // Connect to default 'postgres' database to check/create our target db
    const client = new Client({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: 'postgres', // Default DB
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT,
    });

    try {
        await client.connect();
        console.log('Successfully connected to "postgres" database.');

        const targetDb = process.env.DB_NAME;
        const res = await client.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [targetDb]);

        if (res.rowCount === 0) {
            console.log(`Database "${targetDb}" does not exist. Creating it...`);
            await client.query(`CREATE DATABASE "${targetDb}"`);
            console.log(`Database "${targetDb}" created successfully.`);
        } else {
            console.log(`Database "${targetDb}" already exists.`);
        }

    } catch (err) {
        console.error('Connection failed:', err.message);
        if (err.code === '28P01') {
            console.error('Hint: Authentication failed. Check your DB_PASSWORD in .env');
        } else if (err.code === 'ECONNREFUSED') {
            console.error('Hint: Is PostgreSQL running? Check port 5432.');
        }
    } finally {
        await client.end();
    }
}

checkDatabase();
