const express = require('express');
const dotenv = require('dotenv');
const { processFile } = require('./processor');

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());

app.post('/upload-csv', async (req, res) => {
    try {
        const filePath = process.env.CSV_FILE_PATH;
        if (!filePath) {
            return res.status(500).json({ error: 'CSV_FILE_PATH not configured' });
        }

        // Trigger processing asynchronously
        processFile(filePath)
            .then(() => console.log('Processing completed successfully.'))
            .catch(err => console.error('Processing failed:', err));

        res.status(202).json({ message: 'CSV processing started', file: filePath });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
    console.log(`Send a POST request to /upload-csv to start processing.`);
});
