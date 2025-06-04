public List<Customer> extractCustomersByDeliveryType(String content, String deliveryType) {
    List<Customer> customers = new ArrayList<>();
    String[] lines = content.split("\n");

    for (String line : lines) {
        line = line.trim();
        if (line.isEmpty()) continue;

        String[] fields = line.split("\\|", -1);  // Use -1 to preserve trailing empty fields
        if (fields.length < 5) {
            log.warn("Skipping line due to insufficient fields: {}", line);
            continue;
        }

        // Only process lines starting with "05"
        if ("05".equals(fields[0])) {
            // Check delivery type is in 5th field (index 4)
            if (deliveryType.equalsIgnoreCase(fields[4])) {
                Customer customer = new Customer();
                // Example mappings - adjust as per your Customer class
                customer.setCustomerID(fields[1]);
                customer.setAccountNumber(fields[2]);
                customer.setTenantCode(fields[3]);
                customer.setDeliveryChannel(fields[4]);
                // Add more fields as needed

                customers.add(customer);
                log.info("Extracted customer: {}", customer);
            }
        }
    }
    return customers;
}
