SummaryProcessedFile processedFile = new SummaryProcessedFile();
processedFile.setCustomerId(customer.getCustomerId());
processedFile.setAccountNumber(customer.getAccountNumber());
processedFile.setFullName(customer.getFullName());
processedFile.setEmail(customer.getEmail());
processedFile.setMobileNumber(customer.getMobileNumber());
processedFile.setDueAmount(customer.getDueAmount());
processedFile.setAddressLine1(customer.getAddressLine1());

processedFile.setPdfArchiveFileUrl(pdfArchiveUrl);
processedFile.setPdfEmailFileUrl(pdfEmailUrl);
processedFile.setHtmlEmailFileUrl(htmlEmailUrl);
processedFile.setTxtEmailFileUrl(txtEmailUrl);
processedFile.setPdfMobstatFileUrl(mobstatUrl);
