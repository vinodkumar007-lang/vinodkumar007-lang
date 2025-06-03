package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.CustomerData;

import java.util.ArrayList;
import java.util.List;

public class DataParser {

    /**
     * Parses raw input file content to extract a list of CustomerData objects.
     * This example assumes input lines contain customer info separated by commas.
     */
    public static List<CustomerData> extractCustomerData(String inputFileContent) {
        List<CustomerData> customers = new ArrayList<>();

        if (inputFileContent == null || inputFileContent.isEmpty()) {
            return customers;
        }

        // Example: Each line is a customer record with comma-separated fields
        String[] lines = inputFileContent.split("\\r?\\n");

        for (String line : lines) {
            // Skip empty lines or header lines as needed
            if (line.trim().isEmpty() || line.startsWith("HEADER")) {
                continue;
            }

            String[] fields = line.split(",");

            if (fields.length >= 4) {
                CustomerData customer = new CustomerData();
                customer.setCustomerId(fields[0].trim());
                customer.setAccountNumber(fields[1].trim());
                customer.setName(fields[2].trim());
                customer.setEmail(fields[3].trim());

                // Add any other fields you need here...

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

    /**
     * Generates a simple PDF file for a customer (dummy placeholder).
     * You might want to replace this with actual PDF library usage (e.g., iText or Apache PDFBox).
     */
    public static File generatePdf(CustomerData customer) throws IOException {
        File pdfFile = File.createTempFile(customer.getCustomerId() + "_", ".pdf");

        try (FileWriter writer = new FileWriter(pdfFile)) {
            writer.write("PDF content for customer:\n");
            writer.write("Customer ID: " + customer.getCustomerId() + "\n");
            writer.write("Account Number: " + customer.getAccountNumber() + "\n");
            writer.write("Name: " + customer.getName() + "\n");
        }

        return pdfFile;
    }

    /**
     * Generates a simple HTML file for a customer.
     */
    public static File generateHtml(CustomerData customer) throws IOException {
        File htmlFile = File.createTempFile(customer.getCustomerId() + "_", ".html");

        try (FileWriter writer = new FileWriter(htmlFile)) {
            writer.write("<html><body>");
            writer.write("<h1>Customer Report</h1>");
            writer.write("<p><strong>Customer ID:</strong> " + customer.getCustomerId() + "</p>");
            writer.write("<p><strong>Account Number:</strong> " + customer.getAccountNumber() + "</p>");
            writer.write("<p><strong>Name:</strong> " + customer.getName() + "</p>");
            writer.write("</body></html>");
        }

        return htmlFile;
    }

    /**
     * Generates a simple TXT file for a customer.
     */
    public static File generateTxt(CustomerData customer) throws IOException {
        File txtFile = File.createTempFile(customer.getCustomerId() + "_", ".txt");

        try (FileWriter writer = new FileWriter(txtFile)) {
            writer.write("Customer Report\n");
            writer.write("Customer ID: " + customer.getCustomerId() + "\n");
            writer.write("Account Number: " + customer.getAccountNumber() + "\n");
            writer.write("Name: " + customer.getName() + "\n");
        }

        return txtFile;
    }

    /**
     * Generates a simple MOBSTAT file for a customer.
     * (Format as needed for MOBSTAT system)
     */
    public static File generateMobstat(CustomerData customer) throws IOException {
        File mobstatFile = File.createTempFile(customer.getCustomerId() + "_", ".mobstat");

        try (FileWriter writer = new FileWriter(mobstatFile)) {
            // Example mobstat content format
            writer.write("MOBSTAT Report\n");
            writer.write("ID:" + customer.getCustomerId() + "\n");
            writer.write("ACC:" + customer.getAccountNumber() + "\n");
            writer.write("NAME:" + customer.getName() + "\n");
        }

        return mobstatFile;
    }
}
