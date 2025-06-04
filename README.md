public static List<CustomerData> extractCustomerData(String content, String deliveryType) {
    List<CustomerData> customers = new ArrayList<>();
    String[] lines = content.split("\n");

    for (String line : lines) {
        line = line.trim();
        if (line.isEmpty()) continue;

        // Only process lines starting with "05|"
        if (line.startsWith("05|")) {
            String[] fields = line.split("\\|", -1); // -1 keeps trailing empty fields

            if (deliveryType != null && !deliveryType.equalsIgnoreCase(fields[4])) {
                continue; // Skip if delivery type doesn't match
            }

            try {
                CustomerData customer = new CustomerData();

                customer.setAccountNumber(getField(fields, 1));
                customer.setCustomerId(getField(fields, 2));
                customer.setChannel(getField(fields, 4));
                customer.setLanguage(getField(fields, 5));
                customer.setCurrency(getField(fields, 6));
                customer.setProductCode(getField(fields, 11));
                customer.setFirstName(getField(fields, 21));
                customer.setLastName(getField(fields, 22));
                customer.setFullName(getField(fields, 21) + " " + getField(fields, 22));
                customer.setAddressLine1(getField(fields, 23));
                customer.setAddressLine2(getField(fields, 24));
                customer.setAddressLine3(getField(fields, 25));
                customer.setPostalCode(getField(fields, 29));
                customer.setEmail(getField(fields, 30));
                customer.setMobileNumber(getField(fields, 31));
                customer.setBalance(getField(fields, 32));
                customer.setDueAmount(getField(fields, 34));
                customer.setIdNumber(getField(fields, 40));
                customer.setDeliveryChannel(deliveryType);

                customers.add(customer);
                logger.info("Extracted customer: {}", customer);
            } catch (Exception e) {
                logger.warn("Failed to parse line: {} due to {}", line, e.getMessage());
            }
        }
    }

    return customers;
}

// Helper to safely get field by index
private static String getField(String[] fields, int index) {
    return (fields.length > index) ? fields[index].trim() : "";
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
            writer.write("<p><strong>Customer ID:</strong> " + defaultIfNull(customer.getCustomerId()) + "</p>");
            writer.write("<p><strong>Account Number:</strong> " + defaultIfNull(customer.getAccountNumber()) + "</p>");
            writer.write("<p><strong>Name:</strong> " + getDisplayName(customer) + "</p>");
            writer.write("<p><strong>Email:</strong> " + defaultIfNull(customer.getEmail()) + "</p>");
            writer.write("<p><strong>Mobile:</strong> " + defaultIfNull(customer.getMobileNumber()) + "</p>");
            writer.write("<p><strong>Due Amount:</strong> " + defaultIfNull(customer.getDueAmount()) + "</p>");
            writer.write("<p><strong>Address:</strong> " + defaultIfNull(customer.getAddressLine1()) + "</p>");
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
        sb.append("Customer ID: ").append(defaultIfNull(customer.getCustomerId())).append("\n");
        sb.append("Account Number: ").append(defaultIfNull(customer.getAccountNumber())).append("\n");
        sb.append("Name: ").append(getDisplayName(customer)).append("\n");
        sb.append("Email: ").append(defaultIfNull(customer.getEmail())).append("\n");
        sb.append("Mobile: ").append(defaultIfNull(customer.getMobileNumber())).append("\n");
        sb.append("Due Amount: ").append(defaultIfNull(customer.getDueAmount())).append("\n");
        sb.append("Address Line 1: ").append(defaultIfNull(customer.getAddressLine1())).append("\n");
        return sb.toString();
    }

    private static String getDisplayName(CustomerData customer) {
        if (customer.getFullName() != null && !customer.getFullName().isEmpty()) {
            return customer.getFullName();
        } else {
            String first = defaultIfNull(customer.getFirstName());
            String last = defaultIfNull(customer.getLastName());
            String fullName = (first + " " + last).trim();
            return fullName.isEmpty() ? "" : fullName;
        }
    }

    private static String defaultIfNull(String value) {
        return value != null ? value : "";
    }
}
