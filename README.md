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

public static File generateTxt(CustomerData customer) throws IOException {
    File file = File.createTempFile("customer_", ".txt");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
        writer.write("Account Number: " + customer.getAccountNumber() + "\n");
        writer.write("Customer ID: " + customer.getCustomerId() + "\n");
        writer.write("Name: " + customer.getFullName() + "\n");
        writer.write("Email: " + customer.getEmail() + "\n");
        writer.write("Mobile: " + customer.getMobile() + "\n");
        writer.write("Address: " + customer.getAddressLine1() + ", " + customer.getAddressLine2() + ", " + customer.getAddressLine3() + "\n");
        writer.write("Postal Code: " + customer.getPostalCode() + "\n");
        writer.write("Product Code: " + customer.getProductCode() + "\n");
        writer.write("Balance: " + customer.getBalance() + "\n");
        writer.write("ID Number: " + customer.getIdNumber() + "\n");
    }
    return file;
}
