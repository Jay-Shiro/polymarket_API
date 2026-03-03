const { config } = require("./config");
const { checkSignals } = require("./services/alerts");

let timer = null;

async function runSchedulerCycle() {
  if (!config.schedulerUrl) return;
  try {
    const result = await checkSignals({ url: config.schedulerUrl });
    console.log(`[scheduler] cycle complete: sent=${result.sent_count}`);
  } catch (error) {
    console.error("[scheduler] cycle error:", error.message);
  }
}

function startScheduler() {
  if (!config.enableScheduler || !config.schedulerUrl) {
    return;
  }

  runSchedulerCycle();
  timer = setInterval(runSchedulerCycle, config.schedulerIntervalMs);
  console.log(`[scheduler] started interval=${config.schedulerIntervalMs}ms`);
}

function stopScheduler() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}

module.exports = { startScheduler, stopScheduler };
