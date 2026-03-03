const axios = require("axios");
const { config } = require("../config");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function retryGet(url, params = {}) {
  let lastError = null;
  for (let attempt = 1; attempt <= config.maxRetries; attempt += 1) {
    try {
      const response = await axios.get(url, {
        params,
        timeout: config.requestTimeoutMs
      });
      return response;
    } catch (error) {
      lastError = error;
      if (attempt < config.maxRetries) {
        await sleep(config.retryBackoffMs * attempt);
      }
    }
  }
  throw lastError;
}

module.exports = { retryGet };
