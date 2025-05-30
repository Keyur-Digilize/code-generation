import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();
let superConfig = null;

const getSuperConfig = async ()=> {
    try {
        if(!superConfig){
            superConfig = await prisma.superadmin_configuration.findFirst({ 
                select: {
                    id: true,
                    code_length: true,
                    codes_generated: true,
                    codes_type: true,
                    product_code_length: true,
                    totalCodeGenerated: true,
                    esign_status: true,
                    audit_logs: true,
                    crm_url: true,
                }}
            );
        }
        return superConfig;
    } catch (error) {
        console.log("Error to get super configuration ", error);
        return superConfig;
    }
};

export { getSuperConfig };