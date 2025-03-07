import { schedule } from "node-cron";
import { processRequestedCodes, checkMasterCodeLimit} from './src/codeGeneration.js'; 
console.log("Starting cron job...");

// Schedule the cron job to run every minutes
schedule("*/1 * * * *", processRequestedCodes);

// Schedule the cron job at 00:00 every day
schedule("0 0 * * *", checkMasterCodeLimit);