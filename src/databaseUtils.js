// databaseUtils.js

import prisma from "./prismaClient.js";

const sanitizeTableName = (name) => {
  return name.replace(/[^a-zA-Z0-9_]/g, "");
};

const createDynamicTable = async (tableName, tx) => {
  try {
    const sanitizedTableName = sanitizeTableName(tableName);
    const createTableQuery = `
            CREATE TABLE IF NOT EXISTS "${sanitizedTableName}" (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                serial_no INT NOT NULL UNIQUE REFERENCES "CodesGenerated"(id) ON DELETE CASCADE,
                product_id UUID NOT NULL REFERENCES product(id) ON DELETE CASCADE,
                batch_id UUID NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
                unique_code VARCHAR(255) NOT NULL,
                location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
                code_gen_id VARCHAR(255) NOT NULL,
                country_code VARCHAR(1000) NOT NULL,
                printed BOOLEAN DEFAULT FALSE,
                is_scanned BOOLEAN DEFAULT FALSE,
                is_aggregated BOOLEAN DEFAULT FALSE,
                is_dropped BOOLEAN DEFAULT FALSE,
                parent_id UUID DEFAULT NULL,
                sent_to_cloud BOOLEAN DEFAULT FALSE,
                dropout_reason VARCHAR(20) DEFAULT NULL,
                is_scanned_in_order BOOLEAN DEFAULT FALSE,
                storage_bin INTEGER,
                in_transit BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            );
        `;
    await tx.$executeRawUnsafe(createTableQuery);
    console.log(`Table ${sanitizedTableName} created successfully.`);
  } catch (error) {
    console.error("Error creating table:", error);
    throw error;
  }
};

const checkTableExists = async (tableName, tx = prisma) => {
  const result = await tx.$queryRaw`SELECT EXISTS (
           SELECT 1 
           FROM information_schema.tables 
           WHERE table_schema = 'public' 
           AND table_name = ${tableName}
         ) AS exists;`;

  return result[0]?.exists || false;
};

const createRecordInDynamicTable = async (
  tableName,
  product,
  batch,
  element,
  code,
  countryCode,
  serial_no
) => {
  const uniqueId = `${
    element.generation_id
  }${element.packaging_hierarchy.replace("level", "")}${code.code}`;
  // console.log("unique code ", uniqueId);
  // Using string interpolation for table name, but handle parameters safely
  const query = `
      INSERT INTO ${tableName} 
        (product_id, batch_id, location_id, code_gen_id, unique_code, country_code, serial_no) 
      VALUES ($1, $2, $3, $4, $5, $6, $7);
    `;

  // Execute raw query with parameterized values
  await prisma.$executeRawUnsafe(
    query,
    product.id,
    batch.id,
    batch.location_id,
    element.id,
    uniqueId,
    countryCode,
    serial_no
  );
};

const createRecordsInDynamicTable = async (tableName, data, tx = prisma) => {
  const valuesPlaceholder = data
    .map(
      (_, index) =>
        `($${index * 7 + 1}::uuid, $${index * 7 + 2}::uuid, $${index * 7 + 3}::uuid, $${index * 7 + 4}, $${index * 7 + 5}, $${index * 7 + 6}, $${index * 7 + 7})`
    )
    .join(", ");

  const query = `
    INSERT INTO ${tableName}
    (product_id, batch_id, location_id, code_gen_id, unique_code, country_code, serial_no)
    VALUES ${valuesPlaceholder};
  `;

  const parameters = data.flatMap((item) => [
    item.product_id,
    item.batch_id,
    item.location_id,
    item.code_gen_id,
    item.unique_code,
    item.country_code,
    item.serial_no,
  ]);

  await tx.$executeRawUnsafe(query, ...parameters);
};

const createRecordInCodeSummary = async (data, tx) => {
  const packaging_hierarchy = data.packaging_hierarchy.replace("level", "");
  await tx.codeGenerationSummary.create({
    data: {
      product_id: data.product_id,
      product_name: data.product_name,
      generation_id: data.generation_id,
      packaging_hierarchy: packaging_hierarchy,
      last_generated: String(data.generated),
    },
  });
  // console.log("code summary added", summary);
};

const updateRecordInCodeSummary = async (data, tx) => {
  const packaging_hierarchy = data.packaging_hierarchy.replace("level", "");
  const previousSummaryOfCode = await tx.codeGenerationSummary.findFirst({
    where: {
      product_id: data.product_id,
      generation_id: data.generation_id,
      packaging_hierarchy: packaging_hierarchy,
    },
    select: {
      id: true,
      last_generated: true,
    },
  });
  if (previousSummaryOfCode) {
    const sumOfGenerated =
      data.generated +
      (previousSummaryOfCode?.last_generated
        ? parseInt(previousSummaryOfCode?.last_generated)
        : 0);
    await tx.codeGenerationSummary.update({
      where: { id: previousSummaryOfCode.id },
      data: {
        product_id: data.product_id,
        product_name: data.product_name,
        generation_id: data.generation_id,
        packaging_hierarchy: packaging_hierarchy,
        last_generated: String(sumOfGenerated),
      },
    });
  }
  // console.log("code summary updated", summary);
};

const updateStatusOfCodeRequest = async (id, status, tx) => {
  await tx.codeGenerationRequest.update({
    where: { id },
    data: { status },
  });
  // console.log("status updated", result);
};

const createSsccCodesTable = async (tx = prisma) => {
  try {
    const createTableQuery = `
            CREATE TABLE IF NOT EXISTS "sscc_codes" (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                serial_no INTEGER DEFAULT NULL,
                sscc_code VARCHAR(255) NOT NULL,
                pack_level INTEGER NOT NULL,
                product_id UUID NOT NULL REFERENCES product(id) ON DELETE CASCADE,
                batch_id UUID NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
                product_history_id UUID NOT NULL REFERENCES product_history(id) ON DELETE CASCADE,
                location_id UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
                code_gen_id UUID NOT NULL REFERENCES "CodeGenerationRequest"(id) ON DELETE CASCADE,
                printed BOOLEAN DEFAULT FALSE,
                is_aggregated BOOLEAN DEFAULT FALSE,
                is_dropped BOOLEAN DEFAULT FALSE,
                parent_id UUID DEFAULT NULL,
                sent_to_cloud BOOLEAN DEFAULT FALSE,
                dropout_reason VARCHAR(20) DEFAULT NULL,
                is_scanned_in_order  BOOLEAN DEFAULT FALSE,
                is_opened  BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            );
        `;
    await tx.$executeRawUnsafe(createTableQuery);
    console.log(`Table sscc_codes created successfully.`);
  } catch (error) {
    console.error("Error creating table:", error);
    throw error;
  }
};

const createSsccCodeSummaryTable = async (tx = prisma) => {
  try {
    const createTableQuery = `
            CREATE TABLE IF NOT EXISTS "sscc_code_summary" (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                company_prefix VARCHAR(10) NOT NULL,
                last_generated INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            );
        `;
    await tx.$executeRawUnsafe(createTableQuery);
    console.log(`Table sscc_code_summary created successfully.`);
  } catch (error) {
    console.error("Error creating table:", error);
    throw error;
  }
};

const createRecordsInSsccCodes = async (data, tx) => {
  // Prepare the query for bulk insert
  const valuesPlaceholder = data
    .map(
      (_, index) =>
        `($${index * 7 + 1}, $${index * 7 + 2}, $${index * 7 + 3}::uuid, $${
          index * 7 + 4
        }::uuid, $${index * 7 + 5}::uuid, $${index * 7 + 6}::uuid, $${
          index * 7 + 7
        }::uuid)`
    )
    .join(", ");

  const query = `
        INSERT INTO "sscc_codes"
        (sscc_code, pack_level, product_id, batch_id, product_history_id, location_id, code_gen_id)
        VALUES ${valuesPlaceholder};
    `;

  // Flatten the data array into a single array for parameterized query
  const parameters = data.flat();

  // Execute the query
  await tx.$executeRawUnsafe(query, ...parameters);
};

const createRecordInSsccCodeSummary = async (data, tx = prisma) => {
  await tx.$queryRawUnsafe(`
        INSERT INTO "sscc_code_summary" (
            company_prefix,
            last_generated
        ) 
        VALUES (
            '${data.company_prefix}',                                     
            ${data.no_of_codes}                                    
        );
    `);
};

const updateSsccCodeSummary = async (data, tx = prisma) => {
  const query = `UPDATE "sscc_code_summary"
        SET last_generated = last_generated + ${data.no_of_codes}, updated_at = NOW() 
        WHERE id = '${data.id}'
    `;
  await tx.$queryRawUnsafe(query);
};

export {
  createDynamicTable,
  checkTableExists,
  createRecordInDynamicTable,
  createRecordInCodeSummary,
  updateRecordInCodeSummary,
  updateStatusOfCodeRequest,
  createRecordsInDynamicTable,
  createSsccCodesTable,
  createRecordsInSsccCodes,
  createSsccCodeSummaryTable,
  createRecordInSsccCodeSummary,
  updateSsccCodeSummary,
};
