package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.CustomerData;

import java.util.ArrayList;
import java.util.List;

public class DataParser {

    /**
     * Parses raw input file content to extract a list of CustomerData objects.
     * Assumes each line is formatted as:
     * customerId,accountNumber,name,email,deliveryChannel,mobileNumber,printIndicator,<optional extra fields>
     */
    public static List<CustomerData> extractCustomerData(String inputFileContent) {
        List<CustomerData> customers = new ArrayList<>();

        if (inputFileContent == null || inputFileContent.isEmpty()) {
            return customers;
        }

        String[] lines = inputFileContent.split("\\r?\\n");

        for (String line : lines) {
            if (line.trim().isEmpty() || line.startsWith("HEADER")) {
                continue;
            }

            String[] fields = line.split(",");

            if (fields.length >= 7) {
                CustomerData customer = new CustomerData();
                customer.setCustomerId(fields[0].trim());
                customer.setAccountNumber(fields[1].trim());
                customer.setName(fields[2].trim());
                customer.setEmail(fields[3].trim());
                customer.setDeliveryChannel(fields[4].trim().toUpperCase());
                customer.setMobileNumber(fields[5].trim());
                customer.setPrintIndicator(fields[6].trim());

                // If there are extra tags or custom fields
                if (fields.length > 7) {
                    StringBuilder tagBuilder = new StringBuilder();
                    for (int i = 7; i < fields.length; i++) {
                        tagBuilder.append(fields[i].trim());
                        if (i < fields.length - 1) {
                            tagBuilder.append(",");
                        }
                    }
                    customer.setTags(tagBuilder.toString());
                }

                customers.add(customer);
            }
        }

        return customers;
    }
}
