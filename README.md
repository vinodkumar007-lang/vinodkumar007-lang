package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.CustomerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DataParser {
    private static final Logger logger = LoggerFactory.getLogger(DataParser.class);

    /**
     * Parses raw input file content to extract a list of CustomerData objects.
     * Assumes each line is formatted as:
     * customerId,accountNumber,name,email,deliveryChannel,mobileNumber,printIndicator,<optional extra fields>
     */
    public static List<CustomerData> extractCustomerData(String content, String deliveryType) {
        List<CustomerData> customers = new ArrayList<>();
        String[] lines = content.split("\n");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] field = line.split("\\|", -1);  // Use -1 to preserve trailing empty fields
            if (field.length < 5) {
                logger.warn("Skipping line due to insufficient fields: {}", line);
                continue;
            }

            // Only process lines starting with "05"
            /*if ("05".equals(fields[0])) {
                // Check delivery type is in 5th field (index 4)
                if (deliveryType.equalsIgnoreCase(fields[4])) {
                    CustomerData customer = new CustomerData();
                    // Example mappings - adjust as per your Customer class
                    customer.setCustomerId(fields[1]);
                    customer.setAccountNumber(fields[2]);
                    customer.setTenantCode(fields[3]);
                    customer.setDeliveryChannel(fields[4]);
                    // Add more fields as needed

                    customers.add(customer);
                    logger.info("Extracted customer: {}", customer);
                }
            }*/
            if (line.startsWith("05|")) {
                String[] fields = line.split("\\|", -1); // -1 keeps trailing empty fields

                CustomerData customer = new CustomerData();
                customer.setAccountNumber(fields[1]);
                customer.setCustomerId(fields[2]);
                customer.setChannel(fields[4]);
                customer.setLanguage(fields[5]);
                customer.setCurrency(fields[6]);
                customer.setProductCode(fields[11]);
                customer.setFullName(fields[21] + " " + fields[22]);
                customer.setAddressLine1(fields[23]);
                customer.setAddressLine2(fields[24]);
                customer.setAddressLine3(fields[25]);
                customer.setPostalCode(fields[29]);
                customer.setEmail(fields[30]);
                customer.setMobile(fields[31]);
                customer.setBalance(fields[32]);
                customer.setIdNumber(fields[40]);

                customers.add(customer);
            }
        }
        return customers;
    }

}

package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.CustomerData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileGenerator {

    public static File generatePdf(CustomerData customer) throws IOException {
        File pdfFile = File.createTempFile(customer.getCustomerId() + "_", ".pdf");

        try (FileWriter writer = new FileWriter(pdfFile)) {
            writer.write("PDF content for customer:\n");
            writer.write(buildCustomerContent(customer));
        }

        return pdfFile;
    }

    public static File generateHtml(CustomerData customer) throws IOException {
        File htmlFile = File.createTempFile(customer.getCustomerId() + "_", ".html");

        try (FileWriter writer = new FileWriter(htmlFile)) {
            writer.write("<html><body>");
            writer.write("<h1>Customer Report</h1>");
            writer.write("<p><strong>Customer ID:</strong> " + customer.getCustomerId() + "</p>");
            writer.write("<p><strong>Account Number:</strong> " + customer.getAccountNumber() + "</p>");
            writer.write("<p><strong>Name:</strong> " + customer.getFullName() + "</p>");
            writer.write("<p><strong>Email:</strong> " + customer.getEmail() + "</p>");
            writer.write("<p><strong>Mobile:</strong> " + customer.getMobileNumber() + "</p>");
            writer.write("<p><strong>Due Amount:</strong> " + customer.getDueAmount() + "</p>");
            writer.write("<p><strong>Address:</strong> " + customer.getAddressLine1() + "</p>");
            writer.write("</body></html>");
        }

        return htmlFile;
    }

    public static File generateTxt(CustomerData customer) throws IOException {
        File txtFile = File.createTempFile(customer.getCustomerId() + "_", ".txt");

        try (FileWriter writer = new FileWriter(txtFile)) {
            writer.write("Customer Report\n");
            writer.write(buildCustomerContent(customer));
        }

        return txtFile;
    }

    public static File generateMobstat(CustomerData customer) throws IOException {
        File mobstatFile = File.createTempFile(customer.getCustomerId() + "_", ".mobstat");

        try (FileWriter writer = new FileWriter(mobstatFile)) {
            writer.write("MOBSTAT Report\n");
            writer.write(buildCustomerContent(customer));
        }

        return mobstatFile;
    }

    // Common content builder used by multiple formats
    private static String buildCustomerContent(CustomerData customer) {
        StringBuilder sb = new StringBuilder();
        sb.append("Customer ID: ").append(customer.getCustomerId()).append("\n");
        sb.append("Account Number: ").append(customer.getAccountNumber()).append("\n");
        sb.append("Name: ").append(customer.getFullName()).append("\n");
        sb.append("Email: ").append(customer.getEmail()).append("\n");
        sb.append("Mobile: ").append(customer.getMobileNumber()).append("\n");
        sb.append("Due Amount: ").append(customer.getDueAmount()).append("\n");
        sb.append("Address Line 1: ").append(customer.getAddressLine1()).append("\n");
        return sb.toString();
    }
}

