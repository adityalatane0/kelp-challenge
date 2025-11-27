const CsvParser = require('./parser');
const db = require('./db');
const { setNestedProperty } = require('./utils');

const BATCH_SIZE = 1000;

async function processFile(filePath) {
    console.log(`Starting processing of ${filePath}...`);
    const parser = new CsvParser(filePath);
    let batch = [];
    let count = 0;

    const client = await db.getClient();

    try {
        await client.query('BEGIN');

        // Ensure table exists (in case the SQL script wasn't run manually, though it should be)
        // But for safety/completeness in the app flow:
        await client.query(`
      CREATE TABLE IF NOT EXISTS public.users (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        age INT NOT NULL,
        address JSONB,
        additional_info JSONB
      );
    `);

        for await (const row of parser.parse()) {
            const transformed = transformRow(row);
            batch.push(transformed);

            if (batch.length >= BATCH_SIZE) {
                await insertBatch(client, batch);
                count += batch.length;
                console.log(`Processed ${count} records...`);
                batch = [];
            }
        }

        if (batch.length > 0) {
            await insertBatch(client, batch);
            count += batch.length;
        }

        await client.query('COMMIT');
        console.log(`Successfully processed ${count} records.`);

        await printAgeDistribution(client);

    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Error processing file:', err);
        throw err;
    } finally {
        client.release();
    }
}

function transformRow(flatRow) {
    const result = {
        name: '',
        age: 0,
        address: {},
        additional_info: {}
    };

    const firstName = flatRow['name.firstName'];
    const lastName = flatRow['name.lastName'];
    result.name = `${firstName} ${lastName}`.trim();

    result.age = parseInt(flatRow['age'], 10);

    // Iterate over all keys to distribute them
    for (const [key, value] of Object.entries(flatRow)) {
        if (key === 'name.firstName' || key === 'name.lastName' || key === 'age') {
            continue; // Already handled
        }

        if (key.startsWith('address.')) {
            // Remove 'address.' prefix for the nested object structure? 
            // The prompt says "address jsonb NULL". 
            // Usually we want the structure inside.
            // key is "address.line1". We want result.address.line1 = value.
            // So we pass "line1" to setNestedProperty on result.address?
            // Or just pass "address.line1" to setNestedProperty on result?
            // But result.address is already initialized.

            // Let's use the full key relative to the root of the address object
            const subKey = key.substring('address.'.length);
            setNestedProperty(result.address, subKey, value);
        } else {
            // Everything else goes to additional_info
            setNestedProperty(result.additional_info, key, value);
        }
    }

    return result;
}

async function insertBatch(client, batch) {
    const values = [];
    const placeholders = [];
    let paramIndex = 1;

    batch.forEach(row => {
        values.push(row.name, row.age, row.address, row.additional_info);
        placeholders.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3})`);
        paramIndex += 4;
    });

    const query = `
    INSERT INTO public.users (name, age, address, additional_info)
    VALUES ${placeholders.join(', ')}
  `;

    await client.query(query, values);
}

async function printAgeDistribution(client) {
    const query = `
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
  `;

    const res = await client.query(query);
    const total = res.rows.reduce((sum, row) => sum + parseInt(row.count, 10), 0);

    console.log('\nAge-Group % Distribution');

    // We need to ensure all groups are present or handle them dynamically.
    // The query only returns groups that exist.
    // I'll map the results to the expected format.

    const distribution = {
        '< 20': 0,
        '20 to 40': 0,
        '40 to 60': 0,
        '> 60': 0
    };

    res.rows.forEach(row => {
        if (row.age_group) {
            distribution[row.age_group] = parseInt(row.count, 10);
        }
    });

    for (const [group, count] of Object.entries(distribution)) {
        const percentage = total > 0 ? ((count / total) * 100).toFixed(2) : 0;
        console.log(`${group} ${percentage}`);
    }
}

module.exports = {
    processFile
};
