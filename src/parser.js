const fs = require('fs');
const { Transform } = require('stream');

class CsvParser {
    constructor(filePath) {
        this.filePath = filePath;
        this.headers = null;
    }

    /**
     * Generator function to read and parse the CSV file line by line.
     * Yields JSON objects representing each row.
     */
    async *parse() {
        const stream = fs.createReadStream(this.filePath, { encoding: 'utf8' });
        let buffer = '';

        for await (const chunk of stream) {
            buffer += chunk;
            const lines = buffer.split(/\r?\n/);
            // Keep the last part in the buffer as it might be an incomplete line
            buffer = lines.pop();

            for (const line of lines) {
                if (!line.trim()) continue; // Skip empty lines

                if (!this.headers) {
                    this.headers = this._parseLine(line);
                } else {
                    const values = this._parseLine(line);
                    if (values.length === this.headers.length) {
                        yield this._mapHeadersToValues(this.headers, values);
                    }
                }
            }
        }

        // Process the remaining buffer
        if (buffer.trim()) {
            const line = buffer;
            if (!this.headers) {
                this.headers = this._parseLine(line);
            } else {
                const values = this._parseLine(line);
                if (values.length === this.headers.length) {
                    yield this._mapHeadersToValues(this.headers, values);
                }
            }
        }
    }

    /**
     * Parses a single CSV line into an array of values.
     * Handles quoted values containing commas.
     */
    _parseLine(line) {
        const values = [];
        let currentValue = '';
        let insideQuotes = false;

        for (let i = 0; i < line.length; i++) {
            const char = line[i];

            if (char === '"') {
                insideQuotes = !insideQuotes;
            } else if (char === ',' && !insideQuotes) {
                values.push(currentValue.trim());
                currentValue = '';
            } else {
                currentValue += char;
            }
        }
        values.push(currentValue.trim());
        return values;
    }

    _mapHeadersToValues(headers, values) {
        const obj = {};
        headers.forEach((header, index) => {
            obj[header] = values[index];
        });
        return obj;
    }
}

module.exports = CsvParser;
