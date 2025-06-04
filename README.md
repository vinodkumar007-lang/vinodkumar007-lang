 SummaryProcessedFile processedFile = new SummaryProcessedFile();
                    processedFile.setCustomerID(customer.getCustomerId());
                    processedFile.setAccountNumber(customer.getAccountNumber());
                    processedFile.setFullName(customer.getFullName());
                    processedFile.setEmail(customer.getEmail());
                    processedFile.setMobileNumber(customer.getMobileNumber());
                    processedFile.setDueAmount(customer.getDueAmount());
                    processedFile.setAddressLine1(customer.getAddressLine1());

                    processedFile.setPdfArchiveFileURL(pdfArchiveUrl);
                    processedFile.setPdfEmailFileURL(pdfEmailUrl);
                    processedFile.setHtmlEmailFileURL(htmlEmailUrl);
                    processedFile.setTxtEmailFileURL(txtEmailUrl);
                    processedFile.setPdfMobstatFileURL(mobstatUrl);
