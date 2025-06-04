public static List<CustomerData> extractCustomerData(String content) {
    List<CustomerData> customers = new ArrayList<>();
    String[] lines = content.split("\n");

    for (String line : lines) {
        line = line.trim();
        if (line.isEmpty()) continue;

        if (line.startsWith("05|")) {
            String[] fields = line.split("\\|", -1);

            try {
                CustomerData customer = new CustomerData();

                customer.setAccountNumber(getField(fields, 1));
                customer.setCustomerId(getField(fields, 2));
                customer.setChannel(getField(fields, 4));
                customer.setLanguage(getField(fields, 5));
                customer.setCurrency(getField(fields, 6));
                customer.setProductCode(getField(fields, 11));

                // Corrected indexes for name and address fields:
                customer.setFirstName(getField(fields, 19));
                customer.setLastName(getField(fields, 20));
                customer.setFullName(getField(fields, 19) + " " + getField(fields, 20));
                customer.setAddressLine1(getField(fields, 21));
                customer.setAddressLine2(getField(fields, 22));
                customer.setAddressLine3(getField(fields, 23));

                customer.setPostalCode(getField(fields, 29));
                customer.setEmail(getField(fields, 30));
                customer.setMobileNumber(getField(fields, 31));
                customer.setBalance(getField(fields, 32));
                customer.setDueAmount(getField(fields, 34));
                customer.setIdNumber(getField(fields, 40));

                // Set deliveryChannel to the channel field value
                customer.setDeliveryChannel(getField(fields, 4));

                customers.add(customer);
                logger.info("Extracted customer: {}", customer);
            } catch (Exception e) {
                logger.warn("Failed to parse line: {} due to {}", line, e.getMessage());
            }
        }
    }

    return customers;
}
