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
