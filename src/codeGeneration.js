// product code generation cron
import prisma from './prismaClient.js';
import {
  createDynamicTable,
  checkTableExists,
  createRecordInCodeSummary,
  updateRecordInCodeSummary,
  updateStatusOfCodeRequest,
  createRecordsInDynamicTable,
  createSsccCodesTable,
  createRecordsInSsccCodes,
  createSsccCodeSummaryTable,
  createRecordInSsccCodeSummary,
  updateSsccCodeSummary,
} from "./databaseUtils.js";
import { EXTENSION_DIGIT } from './constant.js'
import { getSuperConfig } from './helper.js';

let reGeneratingCodes = false;
let statusInProgress = false;
const lotSize = 1000;
const ssccLenCheckSum = 17;
const gtinCodeLen = 13;

// Function to generate a six-digit alphanumeric code
const generateCode = (type, length, index) => {
  const alphaNum = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  if (type === "random") {
    return Array.from({ length }, () =>
      alphaNum.charAt(Math.floor(Math.random() * alphaNum.length))
    ).join("");
  } else if (type === "sequential") {
    const sequential = index.toString(36).toUpperCase().padStart(length, "0");
    return sequential;
  }
  return null;
};

const generateSkippedCode = async (reachUpTo, type, codeLength) => {
  const codesCount = await prisma.codesGenerated.count();
  console.log("Previous code count ", codesCount);
  const remainingCodes = [];
  for (let i = 1; i <= reachUpTo; i++) {
    const code = generateCode(type, codeLength, i + codesCount);
    code && remainingCodes.push({ code });
  }
  const generatedCodeRes = await prisma.codesGenerated.createMany({
    data: remainingCodes,
    skipDuplicates: true,
  });
  //   console.log("Generated Skipped Code", generatedCodeRes, remainingCodes);
  return generatedCodeRes;
};

// Generate and insert codes in batches
const generateMasterCodes = async () => {
  console.log("Starting generate code job");
  reGeneratingCodes = true;
  const superConfig = await getSuperConfig();
  try {
    let lotSize = Number(process.env.LOT_SIZE);
    let totalCodes = Number(process.env.TOTAL_CODES);

    if (
      !superConfig.codes_type ||
      (superConfig.codes_type !== "random" &&
        superConfig.codes_type !== "sequential")
    ) {
      console.log(
        "Invalid or missing type field. Type must be 'random' or 'sequential'."
      );
    }

    // Generate codes in batches
    for (let i = 1; i <= totalCodes; i += lotSize) {
      const codes = [];

      for (let j = 0; j < lotSize && i + j <= totalCodes; j++) {
        const code = generateCode(
          superConfig.codes_type,
          superConfig.code_length,
          i + j
        );
        if (code) {
          //   console.log("code", code, "id ", i+j);
          codes.push({ code: code });
        }
      }

      const result = await prisma.codesGenerated
        .createMany({
          data: codes,
          skipDuplicates: true,
        })
        .catch((error) => {
          console.log("Error to generate codes ", error);
        });

      if (result.count != lotSize) {
        console.log("Batch not generated ", result);
        let skipped = lotSize - result.count;
        let archived = result.count;
        while (lotSize !== archived) {
          console.log(`skipped ${skipped} AND archived ${archived} ${lotSize}`);
          const generated = await generateSkippedCode(
            skipped,
            superConfig.codes_type,
            superConfig.code_length
          );
          skipped = skipped - generated.count;
          archived += generated.count;
        }
      } else {
        console.log("Batch generated successfully ", result);
      }
    }

    reGeneratingCodes = false;
    await prisma.superadmin_configuration.update({
      where: {
        id: superConfig.id,
      },
      data: {
        totalCodeGenerated: superConfig.totalCodeGenerated + totalCodes,
      },
    });
  } catch (err) {
    console.error("Error while updating superadmin configuration:", err);
    reGeneratingCodes = false;
  }
};

const calculateGtinCheckDigit = (input) => {
  let sum = 0;
  for (let i = 0; i < gtinCodeLen; i++) {
    const digit = parseInt(input[i]);
    sum += (i + 1) % 2 === 0 ? digit : digit * 3;
  }
  const nearestMultipleOfTen = Math.ceil(sum / 10) * 10;
  const checkDigit = nearestMultipleOfTen - sum;

  console.log("checksum of ", input, " ==> ", checkDigit);
  return checkDigit;
};

const getCountryCode = async ({ codeStructure, ndc, gtin, batchNo, mfgDate, expDate, level, registration_no }) => {
  const elements =
    codeStructure.split("/").length > 1
      ? codeStructure.split("/")
      : codeStructure.split(" ");
  const finalCountryCode = [];
  console.log("element url ", elements);
  for (const element of elements) {
    if (!element) {
      continue;
    }
    switch (element.trim()) {
      case "registrationNo":
        finalCountryCode.push(registration_no);
        break;

      case "NDC":
        finalCountryCode.push(ndc);
        break;

      case "GTIN": {
        const lastDigit = calculateGtinCheckDigit(`${level}${gtin}`);
        finalCountryCode.push(`${level}${gtin}${lastDigit}`);
        break;
      }

      case "batchNo":
        finalCountryCode.push(batchNo);
        break;

      case "manufacturingDate": {
        const date = new Date(mfgDate);
        const formattedDate = `${date
          .getFullYear()
          .toString()
          .slice(2)}${String(date.getMonth() + 1).padStart(2, "0")}${date
          .getDate()
          .toString()
          .padStart(2, "0")}`;
        finalCountryCode.push(formattedDate);
        break;
      }

      case "expiryDate": {
        const date = new Date(expDate);
        const formattedDate = `${date
          .getFullYear()
          .toString()
          .slice(2)}${String(date.getMonth() + 1).padStart(2, "0")}${date
          .getDate()
          .toString()
          .padStart(2, "0")}`;
        finalCountryCode.push(formattedDate);
        break;
      }

      case "<FNC>":
        finalCountryCode.push(String.fromCharCode(29));
        break;

      case "CRMURL":
        const superAdminConfigureData =
          await getSuperConfig();
        finalCountryCode.push(superAdminConfigureData.crm_url);
        break;

      default:
        finalCountryCode.push(element.trim());
        break;
    }
  }
  console.log("final url ", finalCountryCode);
  return codeStructure.split("/").length > 1
    ? finalCountryCode.join("/")
    : finalCountryCode.join("");
};

const calculateSsccCheckDigit = (input) => {
  let sum = 0;
  for (let i = 0; i < ssccLenCheckSum; i++) {
    const digit = parseInt(input[i]);
    sum += (i + 1) % 2 === 0 ? digit : digit * 3;
  }
  const nearestMultipleOfTen = Math.ceil(sum / 10) * 10;
  const checkDigit = nearestMultipleOfTen - sum;

  // console.log("checksum of ", input, " ==> ",  checkDigit);
  return checkDigit;
};

const generateSsccCode = async (data, tx) => {
  console.log("Generate sscc codes data ", data);

  const exists = await checkTableExists("sscc_codes");
  !exists && (await createSsccCodesTable(tx));
  const summaryExists = await checkTableExists("sscc_code_summary");
  !summaryExists && (await createSsccCodeSummaryTable(tx));
  //first check if last generated at exists, then proceed else it will crash here
  let lastGenerated = await tx.$queryRawUnsafe(
    `SELECT last_generated FROM "sscc_code_summary" WHERE company_prefix = '${data.prefix}'`
  );
  console.log("Last generated 5 | 6 ", lastGenerated);
  lastGenerated = lastGenerated.length > 0 && lastGenerated[0]?.last_generated
    ? parseInt(lastGenerated[0].last_generated)
    : 0;
  const codes = [];
  for (let i = 1; i <= data.no_of_codes; i++) {
    const incCount = lastGenerated + i;
    const SIXTEEN_CHAR = (parseInt(data.prefix.padEnd(16, 0)) + incCount).toString();
    const calculatedCheckDigit = calculateSsccCheckDigit(
      `${EXTENSION_DIGIT}${SIXTEEN_CHAR}`
    );
    const sscc_code = `${EXTENSION_DIGIT}${SIXTEEN_CHAR}${calculatedCheckDigit}`;
    codes.push([
      sscc_code,
      data.pack_level,
      data.product_id,
      data.batch_id,
      data.product_history_id,
      data.location_id,
      data.code_gen_id,
    ]);
  }
  // console.log("Finaly data write to db ", codes);
  // Bulk insert
  for (let i = 0; i < codes.length; i += lotSize) {
    const chunk = codes.slice(i, i + lotSize);
    await createRecordsInSsccCodes(chunk, tx);
  }
  await updateStatusOfCodeRequest(data.code_gen_id, "completed", tx);
 //pass in const
 const ssccRecordExistQuery = `SELECT id FROM "sscc_code_summary" WHERE company_prefix = '${data.prefix}'`
  const recordExists = await tx.$queryRawUnsafe(ssccRecordExistQuery);
  console.log("Record exists ", recordExists);
  if (recordExists.length > 0) {
    await updateSsccCodeSummary({
      id: recordExists[0].id,
      no_of_codes: parseInt(data.no_of_codes),
      tx
    });
  } else {
    await createRecordInSsccCodeSummary({
      company_prefix: data.prefix,
      no_of_codes: parseInt(data.no_of_codes),
      tx
    });
  }
};

const insertInBulk = async (data)=> {
  await updateStatusOfCodeRequest(data.elementId, "inprogress", data.tx);
  const countryCode = await getCountryCode({
    codeStructure: data.codeStructure,
    ndc: data.ndc,
    gtin: data.gtin,
    batchNo: data.batchNo,
    mfgDate: data.mfgDate,
    expDate: data.expDate,
    level: data.level
  });
  // Prepare data array
  const codesData = data.codes.map((code) => {
    const uniqueId = `${data.generationId}${data.level}${code.code}`;
    return {
        serial_no: code.id,
        product_id: data.productId,
        batch_id: data.batchId,
        location_id: data.batchLocationId,
        code_gen_id: data.elementId,
        unique_code: uniqueId,
        country_code: countryCode.replaceAll("uniqueCode", uniqueId),
      };
  });
  // Bulk insert
  console.log("Inserting to db...");
  for (let i = 0; i < codesData.length; i += lotSize) {
    const chunk = codesData.slice(i, i + lotSize);
    await createRecordsInDynamicTable(data.tableName, chunk, data.tx);
  }
  console.log("Inserted to db");
  await updateStatusOfCodeRequest(data.elementId, "completed", data.tx);
};

// Reusable function for processing requested codes
//technically a function should have 15 statements. Suggestion : call function inside the loop
const processRequestedCodes = async () => {
  try {
    console.log("Request in progress...");
    const superConfig = await getSuperConfig();

    await prisma.$transaction(async (tx) => {
      if (!statusInProgress) {
        console.log("Cron job started: Processing code generation requests...");
        statusInProgress = true;
        const where = { status: "requested" };
        if (superConfig.esign_status) {
          where.esign_status = "approved"
        }
        const codeGenerationRequests = await tx.codeGenerationRequest.findMany({
          where,
          select: { id: true, product_id: true, batch_id: true, packaging_hierarchy: true, no_of_codes: true, generation_id: true },
          orderBy: { created_at: "asc" },
        });

        for (const element of codeGenerationRequests) {
          console.log("Record requested ", element);
          //returning all columns, when in use are few.
          const product = await tx.product.findFirst({
            where: { id: element.product_id },
            select: { id: true, product_name: true, prefix: true, country_id: true, ndc: true, gtin: true }
          });
          if (!product) {
            console.log("Product not found for request ID:", element.id);
            continue;
          }

          const batch = await tx.batch.findFirst({
            where: { id: element.batch_id },
            select: { id: true, location_id: true, producthistory_uuid: true, batch_no: true, manufacturing_date: true, expiry_date: true }
          });
          if (!batch) {
            console.log("Batch not found for request ID:", element.id);
            continue;
          }


          const LEVEL = element.packaging_hierarchy.replace("level", "");
          const intLevel = parseInt(LEVEL);
          if (intLevel === 5 || intLevel === 6) {
            console.log("Current level ", LEVEL);
            const data = {
              product_id: product.id,
              product_name: product.product_name,
              batch_id: batch.id,
              product_history_id: batch.producthistory_uuid,
              location_id: batch.location_id,
              code_gen_id: element.id,
              prefix: product.prefix,
              no_of_codes: element.no_of_codes,
              pack_level: intLevel,
              packaging_hierarchy: element.packaging_hierarchy,
              generation_id: element.generation_id,
              prefix: product.prefix
            };
            await generateSsccCode(data, tx);
            console.log("Sscc code generated for level ", LEVEL);
          } else {
            const tableName =
              `${element.generation_id}${LEVEL}_CODES`.toLowerCase();
            const exists = await checkTableExists(tableName, tx);
            console.log("Table ", tableName, "exists ", exists);

            const countryCodeStructure = await tx.countryMaster.findFirst({
              where: { id: product.country_id },
              select: { codeStructure: true },
            });

            const codeSummaryData = {
              product_id: product.id,
              product_name: product.product_name,
              packaging_hierarchy: element.packaging_hierarchy,
              generation_id: element.generation_id,
            };

            const insertBulkData = {
              elementId: element.id,
              generationId: element.generation_id,
              codeStructure: countryCodeStructure.codeStructure,
              productId: product.id,
              ndc: product.ndc,
              gtin: product.gtin,
              batchId: batch.id,
              batchNo: batch.batch_no,
              mfgDate: batch.manufacturing_date,
              expDate: batch.expiry_date,
              batchLocationId: batch.location_id,
              level: LEVEL,
              tableName,
              tx
            };

            if (exists) {
              const skipped = await tx.codeGenerationSummary.findFirst({
                where: {
                  product_id: element.product_id,
                  packaging_hierarchy: LEVEL,
                  generation_id: element.generation_id,
                },
                select: { last_generated: true },
              });

              const codes = await tx.codesGenerated.findMany({
                skip: skipped?.last_generated
                  ? parseInt(skipped?.last_generated)
                  : 0,
                take: parseInt(element.no_of_codes),
              });

              await insertInBulk({ ...insertBulkData, codes });
              
              await updateRecordInCodeSummary({
                ...codeSummaryData,
                generated: codes.length,
              }, tx);
              console.log("Done ", LEVEL);
            } else {
              console.log(`Table ${tableName} does not exist. Creating...`);
              await createDynamicTable(tableName, tx);

              const codes = await tx.codesGenerated.findMany({
                select: { id: true, code: true },
                skip: 0,
                take: parseInt(element.no_of_codes),
              });
              console.log("Total codes fetch ", codes.length);

              await insertInBulk({ ...insertBulkData, codes });

              await createRecordInCodeSummary({
                ...codeSummaryData,
                generated: codes.length,
              }, tx);
              console.log("Done level ", LEVEL);
            }
          }
        }
        statusInProgress = false; //why status managed twice? [catch block was used twice as discussed in the conversation]
        console.log("Cron job completed: Code generation requests processed.");
      }
    }, { timeout: 1200000 });
  } catch (error) {
    statusInProgress = false; //suggested by rajan
    console.error("Error processing code generation requests:", error);
  }
};

const checkMasterCodeLimit = async () => {
  const superConfig = await getSuperConfig();
  console.log("Total codes ", superConfig.totalCodeGenerated);
  const nearCodesLimit = (parseInt(superConfig.totalCodeGenerated) * 80) / 100;
  const query = `SELECT * FROM "CodeGenerationSummary" WHERE CAST(last_generated AS INTEGER) >= ${nearCodesLimit} LIMIT 1;`
  const aboveCodesLimit = await prisma.$executeRawUnsafe(query);

  console.log("Near 80% ", aboveCodesLimit);
  if (aboveCodesLimit >= 1 && !reGeneratingCodes) {
    generateMasterCodes();
  }
};

export { processRequestedCodes, checkMasterCodeLimit };

