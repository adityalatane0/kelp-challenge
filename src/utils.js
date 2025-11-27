/**
 * Sets a value in a nested object based on a dot-notation path.
 * @param {Object} obj - The object to modify.
 * @param {string} path - The dot-notation path (e.g., "address.city").
 * @param {any} value - The value to set.
 */
function setNestedProperty(obj, path, value) {
    const keys = path.split('.');
    let current = obj;

    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        // If it's the last key, set the value
        if (i === keys.length - 1) {
            current[key] = value;
        } else {
            // If the key doesn't exist or isn't an object, create it
            if (!current[key] || typeof current[key] !== 'object') {
                current[key] = {};
            }
            current = current[key];
        }
    }
}

module.exports = {
    setNestedProperty,
};
