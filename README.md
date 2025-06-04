SummaryProcessedFile processedFile = new SummaryProcessedFile();
processedFile.setCustomerID(customer.getCustomerId());
processedFile.setAccountNumber(customer.getAccountNumber());
processedFile.setFullName(customer.getFullName());
processedFile.setEmail(customer.getEmail());
processedFile.setPhone(customer.getPhone());
processedFile.setAmountDue(customer.getAmountDue());
processedFile.setAddress(customer.getFullAddress());

processedFile.setPdfArchiveFileURL(pdfArchiveUrl);
processedFile.setPdfEmailFileURL(pdfEmailUrl);
processedFile.setHtmlEmailFileURL(htmlEmailUrl);
processedFile.setTxtEmailFileURL(txtEmailUrl);
processedFile.setPdfMobstatFileURL(mobstatUrl);

processedFile.setStatusCode("OK");
processedFile.setStatusDescription("Success");
