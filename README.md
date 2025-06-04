public static List<CustomerData> extractCustomerData(String content) {
    List<CustomerData> customers = new ArrayList<>();
    String[] lines = content.split("\n");

    for (String line : lines) {
        line = line.trim();
        if (line.isEmpty()) continue;

        // Only process lines starting with "05|"
        if (line.startsWith("05|")) {
            String[] fields = line.split("\\|", -1); // -1 keeps trailing empty fields

            try {
                CustomerData customer = new CustomerData();

                customer.setAccountNumber(getField(fields, 1));
                customer.setCustomerId(getField(fields, 2));
                customer.setChannel(getField(fields, 4));  // Delivery channel
                customer.setLanguage(getField(fields, 5));
                customer.setCurrency(getField(fields, 6));
                customer.setProductCode(getField(fields, 11));

                String firstName = getField(fields, 21);
                String lastName = getField(fields, 22);
                customer.setFirstName(firstName);
                customer.setLastName(lastName);

                if (!firstName.isEmpty() && !lastName.isEmpty()) {
                    customer.setFullName(firstName + " " + lastName);
                } else if (!firstName.isEmpty()) {
                    customer.setFullName(firstName);
                } else if (!lastName.isEmpty()) {
                    customer.setFullName(lastName);
                } else {
                    customer.setFullName("");
                }

                customer.setAddressLine1(getField(fields, 23));
                customer.setAddressLine2(getField(fields, 24));
                customer.setAddressLine3(getField(fields, 25));
                customer.setPostalCode(getField(fields, 29));
                customer.setEmail(getField(fields, 30));
                customer.setMobileNumber(getField(fields, 31));
                customer.setBalance(getField(fields, 32));
                customer.setDueAmount(getField(fields, 34));
                customer.setIdNumber(getField(fields, 40));

                // Since no deliveryType param, just set channel value as deliveryChannel
                customer.setDeliveryChannel(customer.getChannel());

                customers.add(customer);
                logger.info("Extracted customer: {}", customer);
            } catch (Exception e) {
                logger.warn("Failed to parse line: {} due to {}", line, e.getMessage());
            }
        }
    }

    return customers;
}

// Keep the getField method as before
private static String getField(String[] fields, int index) {
    if (fields != null && index >= 0 && index < fields.length) {
        return fields[index] != null ? fields[index].trim() : "";
    }
    return "";
}
